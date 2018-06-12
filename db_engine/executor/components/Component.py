from abc import ABCMeta, abstractmethod
import datetime
from db_model.models import Task, TaskRelies
from executor import TASK_STATUS
from common.UTIL import mk_working_directory, COMPONENTS
import pickle
import os
import setting, cluster_setting

FULL_EXECUTION = 'FULL_EXECUTION'
EXECUTABLE = [COMPONENTS.ROBOTX_SPARK,COMPONENTS.FEATURE_COMBINE,COMPONENTS.ATOM_LEARN,COMPONENTS.ATOM_ACT,COMPONENTS.ATOM_TEST]
CONT_EXECUTION = 'CONT_EXECUTION'
SING_EXECUTION = 'SING_EXECUTION'


class Component(object):
    __metaclass__ = ABCMeta

    PREVIOUS = "PREVIOUS.PKL"

    YARN_LOG_NAME = "LOG.HEX"

    COMPONENT_TYPE = None

    TASK_RELY = None

    DEFAULT_DRIVER_MEMORY = cluster_setting.DEFAULT_DRIVER_MEMORY

    DEFAULT_NUM_EXECUTORS = cluster_setting.DEFAULT_NUM_EXECUTORS

    DEFAULT_EXECUTOR_MEMORY = cluster_setting.DEFAULT_EXECUTOR_MEMORY

    def __init__(self, project_id, component_id):
        self.project_id = project_id
        self.component_id = component_id
        self.loaded = False

    def need_execution(self, force=False):
        changed = True if force else self.changed()
        if changed:
            if not self.loaded:
                self.load_from_db()
            pickle_path = mk_working_directory(self.project_id, self.component_id, Component.PREVIOUS)
            with open(pickle_path, 'wb') as f:
                pickle.dump(self, f)
            self.prepare()
        return changed

    def execute(self, task_id):
        res = self.TASK_RELY.delay(project_id=self.project_id, component_id=self.component_id, task_id= task_id)
        # 记录任务提交时间
        submit_time = datetime.datetime.now()
        Task.objects.filter(project_id=self.project_id, component_id=self.component_id) \
            .update(submit_time=submit_time, task_status=TASK_STATUS.SUBMITTED, celery_id=res.id)

    def record(self, task_id, relies, forwards):
        record_time = datetime.datetime.now()
        Task.objects.update_or_create(project_id=self.project_id,
                                      component_id=self.component_id,
                                      defaults=dict(
                                          task_id=task_id,
                                          component_type=self.COMPONENT_TYPE,
                                          error_code=None,
                                          application_id=None,
                                          tracking_url=None,
                                          has_log=False,
                                          task_status=TASK_STATUS.PENDING,
                                          relies=relies,
                                          submit_time=None,
                                          record_time=record_time,
                                          detail=None,
                                          start_time=None,
                                          end_time=None
                                      )
                                      )
        # 保存依赖关系
        if forwards is not None:
            task_relies = list()
            for forward in forwards:
                task_relies.append(
                    TaskRelies(project_id=self.project_id, sc_comp_id=self.component_id, tg_comp_id=forward))
            TaskRelies.objects.bulk_create(task_relies)

    def changed(self):
        project_id = self.project_id
        component_id = self.component_id
        previous_pickle_path = os.path.join(setting.WORKING_DIRECTORY, project_id, component_id, self.PREVIOUS)
        if not os.path.exists(previous_pickle_path):
            return True
        # 查询之前执行状态
        previous_execute_task = Task.objects.filter(project_id=self.project_id, component_id=self.component_id)
        if len(previous_execute_task) == 0:
            return True
        previous_execute_task = previous_execute_task[0]
        assert isinstance(previous_execute_task, Task)
        if previous_execute_task.task_status != TASK_STATUS.SUCCEEDED:
            return True
        with open(previous_pickle_path, 'rb') as f:
            previous_component = pickle.load(f)
            self.load_from_db()
            return not self == previous_component

    def evaluate_resource(self):
        return self.DEFAULT_DRIVER_MEMORY, self.DEFAULT_NUM_EXECUTORS, self.DEFAULT_EXECUTOR_MEMORY

    def load_from_db(self):
        """
        load configuration from database
        :return:
        """
        self.__load_from_db__()
        self.loaded = True

    @abstractmethod
    def __load_from_db__(self):
        """
        load configuration from database
        :return:
        """
        pass

    @abstractmethod
    def prepare(self):
        """
        prepare configuration files
        :return:
        """
        pass

    @staticmethod
    def get_yarn_log_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, Component.YARN_LOG_NAME)

    @staticmethod
    def fetch_log(project_id, component_id):
        yarn_log_path = Component.get_yarn_log_path(project_id, component_id)
        logs = list()
        with open(yarn_log_path, "rb") as f:
            t_logs = f.readlines()
            for line in t_logs:
                try:
                    logs.append(line.decode("utf-8"))
                except Exception: pass
        return "".join(logs)

    @staticmethod
    def cluster_working_directory(project_id, component_id, *external):
        return "%s/%s/%s" %(cluster_setting.CLUSTER_WORKING_DIRECTORY, project_id, component_id)
