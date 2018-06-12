import json
import os
import xml.etree.ElementTree as et
from collections import OrderedDict
from setting import WORKING_DIRECTORY
from F_SETTING import CLUSTER_DIRECTORY

import pyhdfs
import requests
import time
from django.http import HttpResponse

from common import ERRORS
from db_engine import celery_app
from executor.components.Component import *

from common.UTIL import auto_param, Response
from db_engine.xml import save_xml
from executor.topology.Topology import Toplogy
from db_model.models import TaskIDGenerator, Execution, CurrentExecution
from executor.TASK_STATUS import ExecutionStatus
from executor.components.FeatureCombine import FeatureCombine
from executor.components.RobotXSpark import RobotXSpark
from executor.components.AtomLearn import AtomLearn
from executor.components.AtomAct import AtomAct
from executor.components.AtomTest import AtomTest
from cluster_setting import APPLICATION_KILL_URL
import logging

logger = logging.getLogger("django")


def get_task_id(project_id):
    query_result = TaskIDGenerator.objects.filter(project_id=project_id)
    if len(query_result) == 0:
        # 没有，task_id返回0，并保存记录到数据库
        TaskIDGenerator(project_id=project_id, task_id=1).save()
        return "%s_%d" % (project_id, 0)
    else:
        # 有，task_id + 0 到 task_id + n -1，返回，并更新数据库
        task_id_generator = query_result[0]
        task_id = task_id_generator.task_id
        task_id_generator.task_id = task_id_generator.task_id + 1
        task_id_generator.save()
        return "%s_%d" % (project_id, task_id)


@auto_param
def execute(request, project_id, xml, execution_type, execute_id=None):
    execution = CurrentExecution.objects.filter(project_id=project_id)
    if len(execution) != 0:
        execution = execution[0]
        if execution.current_execution is not None:
            return HttpResponse(Response.fail(ERRORS.TASK_IS_EXECUTING).to_json())

    save_xml(project_id, xml)

    root = et.fromstring(xml)
    flows = root.findall("./sequenceFlow")
    topology = Toplogy()
    for flow in flows:
        start = flow.get('sourceRefs')
        end = flow.get('targetRefs')
        topology.add_line(start, end)

    topology.sort()
    # 根据不同执行类型，构造不同的需要执行的组件
    try:
        if execution_type == FULL_EXECUTION:
            levels = topology.levels
            need_execute = OrderedDict()
            # 获取需要执行的组件
            for level in levels:
                for point in level:
                    if point.type in EXECUTABLE:
                        # 生成需要执行 配置文件
                        executor_class = eval(point.type)
                        executor = executor_class(project_id, point.id)
                        executor.need_execution(force=True)
                        need_execute[point.id] = point
            task_id = execute_components(need_execute, project_id)
        elif execution_type == CONT_EXECUTION:
            levels = topology.levels
            # 记录需要执行的组件
            need_execute = get_need_execute(levels, project_id)
            task_id = execute_components(need_execute, project_id)
        elif execution_type == SING_EXECUTION:
            levels, flat_points = topology.get_previous_component(execute_id)

            need_execute = get_need_execute(levels[:-1], project_id, flat_points)
            execute_point = list(levels[-1])[0]
            need_execute[execute_id] = execute_point
            executor_class = eval(execute_point.type)
            executor = executor_class(project_id, execute_point.id)
            executor.need_execution(force=True)
            task_id = execute_components(need_execute, project_id)
    except Exception as e:
        error_code = str(e)
        return HttpResponse(Response.fail(error_code, None).to_json())
    return HttpResponse(Response.success(task_id).to_json())


def need_execution_add(forwards, need_execute, project_id, limit):
    for forward in forwards:
        if limit is None or forward.id in limit:
            if forward.id not in need_execute:
                need_execute[forward.id] = forward
                executor_class = eval(forward.type)
                executor = executor_class(project_id, forward.id)
                executor.need_execution(force=True)
            need_execution_add(forward.forwards, need_execute, project_id, limit)


def execute_components(need_execute, project_id):
    if len(need_execute) == 0:
        # 没有需要执行的组件
        return None
    # get task_id
    task_id = get_task_id(project_id)
    # 之前任务删除，之前依赖删除
    TaskRelies.objects.filter(project_id=project_id).delete()
    for point in need_execute.values():
        relies = 0
        for rely_point in point.relies_bak:
            if rely_point.id in need_execute:
                relies += 1
        executor_class = eval(point.type)
        forwards = None if len(point.forwards) == 0 else [p.id for p in point.forwards]
        executor = executor_class(project_id, point.id)
        assert isinstance(executor, Component)
        # 记录数据库
        executor.record(task_id, relies, forwards)
    Execution(project_id=project_id, task_id=task_id, start_time=datetime.datetime.now(),
              status=ExecutionStatus.RUNNING, task_count=len(need_execute)).save()
    CurrentExecution.objects.update_or_create(project_id=project_id, defaults=dict(
        current_execution=task_id
    ))
    return task_id


def get_need_execute(levels, project_id, limit=None):
    need_execute = OrderedDict()
    for level in levels:
        # 记录需要执行的组件， 用于计算 relies
        for point in level:
            if point.type not in EXECUTABLE:
                # 如果不可执行，跳过
                continue
            if point.id in need_execute:
                continue
            executor_class = eval(point.type)
            executor = executor_class(project_id, point.id)
            comp_need_execute = executor.need_execution()
            if comp_need_execute:
                need_execute[point.id] = point
                # 把后置组件加入need_execution中, limit用于在单点执行时做限制
                need_execution_add(point.forwards, need_execute, project_id, limit)
    return need_execute


class ExecutionQuery(object):
    def __init__(self, status, start_time, end_time):
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.detail = list()

    def add_detail(self, component_id, task_status, detail, error_code, application_id, tracking_url, has_log,
                   start_time, end_time):
        self.detail.append(dict(
            component_id=component_id,
            task_status=task_status,
            error_code=error_code,
            application_id=application_id,
            tracking_url=tracking_url,
            detail=detail,
            has_log=has_log,
            start_time=start_time,
            end_time=end_time
        ))


@auto_param
def execution_status(request, project_id, task_id):
    execution = Execution.objects.filter(project_id=project_id, task_id=task_id)
    if len(execution) == 0:
        return HttpResponse(Response.fail(ERRORS.NO_SUCH_TASK).to_json())
    execution = execution[0]
    tasks = Task.objects.order_by('record_time').filter(project_id=project_id, task_id=task_id)
    query = ExecutionQuery(execution.status, execution.start_time, execution.end_time)
    for task in tasks:
        assert isinstance(task, Task)
        component_id = task.component_id
        task_status = task.task_status
        detail = task.detail
        has_log = task.has_log
        application_id = task.application_id
        error_code = task.error_code
        tracking_url = task.tracking_url
        start_time = task.start_time
        end_time = task.end_time
        query.add_detail(component_id, task_status, detail, error_code, application_id, tracking_url, has_log,
                         start_time, end_time)
    if execution.status != ExecutionStatus.RUNNING:
        CurrentExecution.objects.filter(project_id=project_id).update(current_execution=None)
    return HttpResponse(Response.success(query).to_json())


@auto_param
def current_execution(request, project_id):
    execution = CurrentExecution.objects.filter(project_id=project_id)
    if len(execution) == 0:
        return HttpResponse(Response.success().to_json())
    execution = execution[0]  # type: CurrentExecution
    if execution.current_execution is None:
        task_status_list = list()
        tasks = Task.objects.order_by('record_time').filter(project_id=project_id)
        for task in tasks:
            assert isinstance(task, Task)
            component_id = task.component_id
            task_status = task.task_status
            detail = task.detail
            has_log = task.has_log
            application_id = task.application_id
            error_code = task.error_code
            tracking_url = task.tracking_url
            start_time = task.start_time
            end_time = task.end_time
            task_status_list.append(dict(
                component_id=component_id,
                task_status=task_status,
                error_code=error_code,
                application_id=application_id,
                tracking_url=tracking_url,
                detail=detail,
                has_log=has_log,
                start_time=start_time,
                end_time=end_time
            ))
        return HttpResponse(Response.success(task_status_list).to_json())
    return HttpResponse(Response.success(execution.current_execution).to_json())


@auto_param
def get_log(request, project_id, component_id):
    task = Task.objects.filter(project_id=project_id, component_id=component_id)
    if len(task) == 0:
        return HttpResponse(Response.fail(ERRORS.NO_SUCH_TASK).to_json())
    task = task[0]
    if not task.has_log:
        return HttpResponse(Response.fail(ERRORS.TASK_HAS_NO_LOG).to_json())
    return HttpResponse(Response.success(Component.fetch_log(project_id, component_id)).to_json())


@auto_param
def kill_task(request, app_id):
    kill_url = APPLICATION_KILL_URL.format(app_id=app_id)
    r = requests.put(kill_url, '{"state": "KILLED"}', headers={'Content-Type': 'application/json'})
    if r.status_code == 404:
        return HttpResponse(Response.success("NO_SUCH_APP").to_json())
    elif r.status_code == 200 or r.status_code == 202:
        state = json.loads(r.content.decode())['state']
        if state in ["ACCEPTED", "RUNNING"]:
            return HttpResponse(Response.success("TASK_KILLED").to_json())
        else:
            return HttpResponse(Response.success("TASK_ALREADY_FINISHED").to_json())
    else:
        return HttpResponse(Response.success("APP_ID_ERROR").to_json())


@auto_param
def stop_all(request, project_id):
    current = CurrentExecution.objects.filter(project_id=project_id)  # type:list[CurrentExecution]
    if len(current) == 0:
        return HttpResponse(Response.success("CURRENT_PROJECT_IS_NOT_EXECUTING").to_json())
    task_id = current[0].current_execution
    execution = Execution.objects.filter(project_id=project_id, task_id=task_id)
    if execution[0].status != ExecutionStatus.RUNNING:
        return HttpResponse(Response.success("CURRENT_PROJECT_IS_NOT_EXECUTING").to_json())
    tasks = Task.objects.filter(project_id=project_id, task_id=task_id, task_status__in=[TASK_STATUS.PENDING,
                                                                                        TASK_STATUS.SUBMITTED,
                                                                                        TASK_STATUS.RUNNING
                                                                                        ])    # type: list[Task]
    running_without_app = list()
    for task in tasks:
        if task.task_status == TASK_STATUS.PENDING:
            task.relies = task.relies + 1
            task.save()
            logger.info("task[%s-%s-%s] PENDING CANCEL" % (project_id, task.component_id, task_id))
        elif task.task_status == TASK_STATUS.SUBMITTED:
            celery_app.control.revoke(task.celery_id, terminate=True)
            task.task_status = TASK_STATUS.KILLED
            task.save()
            end_time = datetime.datetime.now()
            Execution.objects.filter(project_id=project_id, task_id=task_id).update(
                status=TASK_STATUS.ExecutionStatus.FAILED, end_time=end_time)
            logger.info("task[%s-%s-%s] SUBMMITED REVOKE" %(project_id, task.component_id, task_id))
        elif task.task_status == TASK_STATUS.RUNNING:
            if task.application_id is not None:
                kill_url = APPLICATION_KILL_URL.format(app_id=task.application_id)
                requests.put(kill_url, '{"state": "KILLED"}', headers={'Content-Type': 'application/json'})
                logger.info("task[%s-%s-%s] RUNNING KILLED" % (project_id, task.component_id, task_id))
            else:
                running_without_app.append(task.component_id)
    while len(running_without_app) !=0:
        time.sleep(3)
        tasks = Task.objects.filter(project_id=project_id, task_id=task_id,
                                    task_status=TASK_STATUS.RUNNING, component_id__in=running_without_app)
        if len(tasks) == 0:
            break
        running_without_app = list()
        for task in tasks:
            if task.application_id is not None:
                kill_url = APPLICATION_KILL_URL.format(app_id=task.application_id)
                requests.put(kill_url, '{"state": "KILLED"}', headers={'Content-Type': 'application/json'})
                logger.info("task[%s-%s-%s] RUNNING KILLED" % (project_id, task.component_id, task_id))
            else:
                running_without_app.append(task.component_id)
    return HttpResponse(Response.success().to_json())
