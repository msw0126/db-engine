import datetime
import os
import subprocess
from functools import wraps

import requests
from celery import shared_task
from django.db.models import F
from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JNetworkError, Py4JJavaError
from django.db import connections

import cluster_setting
from F_SETTING import CLUSTER_COMPONENT_DIR
from executor.components.Component import Component
from common import ERRORS
from common.UTIL import extract_component_type, to_json
from db_model.models import Task, TaskRelies, Execution
from db_engine.yarn_resource import yarn_resource
from executor import TASK_STATUS
from executor.status_query import LogQuery
from lxml import etree
from urllib import parse
from requests.exceptions import ConnectionError
import time


def connection_check():
    for conn in connections.all():
        if conn.connection is not None:
            conn.is_usable()


def download_log(project_id, component_id, task_id, app_id, user):
    download_path = Component.get_yarn_log_path(project_id, component_id)
    try:
        gateway = JavaGateway()
        hive_util = gateway.entry_point.getHiveUtil()
        func = hive_util.downLoadLog
        func(app_id,user,download_path)
    except Py4JNetworkError as e:
        return False
    except Py4JJavaError as e:
        return False
    connection_check()
    Task.objects.filter(project_id=project_id, component_id=component_id, task_id=task_id).update(has_log=True)
    return True


def common_env_setting():
    # 添加环境变量
    os.environ.setdefault("HADOOP_CONF_DIR", cluster_setting.HADOOP_CONFIG)
    os.environ.setdefault("HADOOP_USER_NAME", cluster_setting.HADOOP_USER_NAME)
    os.environ.setdefault("YARN_CONF_DIR", cluster_setting.HADOOP_CONFIG)
    os.environ.setdefault("SPARK_CLASSPATH", cluster_setting.SPARK_CLASSPATH)
    if cluster_setting.PYSPARK_PYTHON is not None:
        os.environ.setdefault("PYSPARK_PYTHON", cluster_setting.PYSPARK_PYTHON)
    os.environ.setdefault( "PYSPARK_PYTHON", "./miniconda2.tar.gz/miniconda2/bin/python")


def common_setting(project_id, component_id):
    common_env_setting()
    resource = yarn_resource(project_id, component_id)
    command = [cluster_setting.SPARK_PATH,
               "--master", "yarn",
               "--deploy-mode", "cluster",
               "--name", "[%s][%s]" % (project_id, component_id),
               "--driver-memory", "%dG" % resource.driver_memory,
               "--num-executors", str(resource.num_executor),
               "--executor-memory", "%dG" % resource.executor_memory,
               "--executor-cores", str(resource.executor_cores),
               "--conf", "spark.driver.extraJavaOptions=-XX:MaxPermSize=%dM" %resource.driver_perm,
               "--conf", "spark.executor.extraJavaOptions=-XX:MaxPermSize=%dM" % resource.executor_perm,
               "--conf", "spark.yarn.dist.archives=%s/miniconda2.tar.gz" % CLUSTER_COMPONENT_DIR,
               "--conf", "spark.executorEnv.PYSPARK_PYTHON=./miniconda2.tar.gz/miniconda2/bin/python"]
    return command


def download_report(f, t):
    gateway = None
    try:
        gateway = JavaGateway()
        ff = gateway.jvm.java.util.ArrayList()
        tt = gateway.jvm.java.util.ArrayList()
        for k in f:
            ff.append(k)
        for k in t:
            tt.append(k)
        hive_util = gateway.entry_point.getHiveUtil()
        func = hive_util.downloadFiles
        func(ff, tt)
    except Exception as e:
        raise e
    finally:
        if gateway is not None:
            gateway.close()


def task_recorder(func):
    @wraps(func)
    def wrapper(*arg, **kwargs):
        project_id = kwargs['project_id']
        component_id = kwargs['component_id']
        task_id = kwargs['task_id']
        start_time = datetime.datetime.now()
        # 更新状态为运行
        connection_check()
        Task.objects.filter(project_id=project_id, component_id=component_id).update(start_time=start_time,
                                                                                     task_status=TASK_STATUS.RUNNING)
        res = func(*arg, **kwargs)
        end_time = datetime.datetime.now()
        connection_check()
        Task.objects.filter(project_id=project_id, component_id=component_id).update(end_time=end_time, task_status=res)

        if res == TASK_STATUS.SUCCEEDED:
            task_count = Execution.objects.filter(project_id=project_id, task_id=task_id)[0].task_count
            task_success_count = Task.objects.filter(project_id=project_id, task_id=task_id, task_status=TASK_STATUS.SUCCEEDED).count()
            if task_success_count==task_count:
                Execution.objects.filter(project_id=project_id, task_id=task_id).update(
                    status=TASK_STATUS.ExecutionStatus.SUCCEEDED, end_time=end_time)
            else:
                # 更新其他组件依赖状态
                forwards = TaskRelies.objects.filter(project_id=project_id, sc_comp_id=component_id)
                forwards = [forward.tg_comp_id for forward in forwards]
                Task.objects.filter(project_id=project_id, component_id__in=forwards).update(relies=F('relies') - 1)
        else:
            Execution.objects.filter(project_id=project_id, task_id=task_id).update(
                status=TASK_STATUS.ExecutionStatus.FAILED, end_time=end_time)
        return res
    return wrapper


def update_task_detail(project_id, component_id, task_id, error_code=None, detail=None, application_id=None, tracking_url=None):
    update_dict = dict()
    if error_code is not None:
        update_dict['error_code'] = error_code
    if detail is not None:
        update_dict['detail'] = detail
    if application_id is not None:
        update_dict['application_id'] = application_id
    if tracking_url is not None:
        update_dict['tracking_url'] = tracking_url
    if len(update_dict) ==0 :
        return
    connection_check()
    Task.objects.filter(project_id= project_id, component_id= component_id, task_id=task_id).update(**update_dict)


def log_to(cnt, project_id, component_id, task_id, type_):
    with open(os.path.join("/home/wj/tzb_databrain/debug_info", "%s_%s_%s_%s.html" %(project_id, component_id, task_id, type_)), 'w') as f:
        f.write(cnt)


def fetch_log(job_server_log_query_url, project_id, component_id, task_id):
    """
    extract log from html and update database
    :param job_server_log_query_url:
    :param project_id:
    :param component_id:
    :param task_id:
    :return:
    """

    if job_server_log_query_url is None: return

    def retry_fetch(n=5):
        print("fetch %d times:[%s]" %(n, job_server_log_query_url))
        if n==0:
            return None
        r = requests.get(job_server_log_query_url)
        cnt = r.content.decode()
        selector = etree.HTML(cnt)
        # log_to(cnt,project_id,component_id,task_id,'cnt_%d' %n)
        content = selector.xpath("//td[@class='content']/*/text()")
        if len(content) == 0 \
                or ('No logs available for container' in content[0])\
                or ('Logs not available for' in content[0]):
            time.sleep(5)
            return retry_fetch(n-1)
        return selector

    selector = retry_fetch()

    if selector is None:
        raise Exception("log query failed, max retry 5 exceeded")

    res = selector.xpath("//td[@class='content']/p/a[text()='here']/@href")
    link_stderr = None
    link_stdout = None
    for link in res:
        if "stderr" in link:
            link_stderr = link
        elif "stdout" in link:
            link_stdout = link

    host,rest = parse.splithost(job_server_log_query_url.replace("http:",""))

    stderr = None
    if link_stderr is None:
        logs = selector.xpath("//td[@class='content']/p[contains(text(),'Log Type: stdout')]/preceding-sibling::*/text()")
        logs = [l.strip() for l in logs]
        logs = [l for l in logs if l!='']
        stderr = "\n".join(logs)
    else:
        link_stderr = "http://%s%s" %(host, link_stderr)
        err_r = requests.get(link_stderr)
        err_cnt = err_r.content.decode()
        err_sel = etree.HTML(err_cnt)
        # log_to(err_cnt, project_id, component_id, task_id, 'err')
        logs = err_sel.xpath("//td[@class='content']//text()")
        logs = [l.strip() for l in logs]
        stderr = "\n".join(logs)

    stdout = None
    if link_stdout is None:
        logs = selector.xpath("//td[@class='content']/p[contains(text(),'Log Type: stdout')]/following-sibling::*//text()")
        logs = ['Log Type: stdout'] + [l.strip() for l in logs]
        stdout = "\n".join(logs)
    else:
        link_stdout = "http://%s%s" %(host, link_stdout)
        out_r = requests.get(link_stdout)
        out_cnt = out_r.content.decode()
        out_sel = etree.HTML(out_cnt)
        # log_to(out_cnt, project_id, component_id, task_id, 'out')
        logs = out_sel.xpath("//td[@class='content']//text()")
        logs = [l.strip() for l in logs]
        stdout = "\n".join(logs)

    download_path = Component.get_yarn_log_path(project_id, component_id)
    with open(download_path, "w") as f:
        f.write(stdout)
        f.write("\n")
        f.write(stderr)
    Task.objects.filter(project_id=project_id, component_id=component_id, task_id=task_id).update(has_log=True)


def spark_submit(project_id, component_id, task_id, p):
    application_id = None
    tracking_url = None
    error_line = list()
    error_code = None
    submit_log = list()
    while p.poll() is None:
        line = p.stderr.readline()
        print(line)
        line = line.decode('utf-8', 'ignore').strip()
        if len(error_line) != 0:
            if len(line) > 0: error_line.append(line)
            continue
        elif len(line) > 0 and "above the max threshold" in line:
            error_code = ERRORS.YARN_RESOURCE_EXCEED
            error_line.append(line)
            continue
        elif len(line) > 0 and "Already tried 9 time(s)" in line:
            if len(line) > 0: error_line.append(line)
            break

        if len(line)>0:
            submit_log.append(line)
        if len(line) > 0 and (application_id is None or tracking_url is None):
            assert isinstance(line, str)
            if line.startswith("tracking URL:"):
                tracking_url = line.replace("tracking URL:", "").strip()
            elif "Submitted application" in line:
                application_id = line.split("Submitted application")[1].strip()

        if application_id is not None and tracking_url is not None:
            # 表示已经提交成功
            # 可以开始查询任务执行状况
            p.kill()

    if error_code is not None:
        # 任务提交时发生错误
        update_task_detail(project_id, component_id, task_id, error_code=error_code, detail="\n".join(error_line))
        return TASK_STATUS.FAILED
    elif application_id is None:
        update_task_detail(project_id, component_id, task_id, detail="\n".join(submit_log))
        return TASK_STATUS.FAILED
    update_task_detail(project_id, component_id, task_id, application_id=application_id, tracking_url=tracking_url)

    process_recorder = LogQuery.ProcessRecorder(project_id, component_id, task_id, update_task_detail)
    error_extractor = LogQuery.ErrorRecorder(project_id, component_id, task_id, update_task_detail)
    try:
        state_final,job_server_log_query_url  = LogQuery.query(application_id, process_recorder, error_extractor)
        # download_log(project_id, component_id, task_id, application_id, cluster_setting.HADOOP_USER_NAME)
    except ConnectionError as e:
        update_task_detail(project_id, component_id,task_id,error_code="STATUS_QUERY_ERROR",detail="mainly can not connect to http server of yarn application server\n"+str(e))
        return TASK_STATUS.FAILED
    except Execution as e:
        update_task_detail(project_id, component_id, task_id, error_code="UNKNOWN_ERROR",detail=str(e))
        return TASK_STATUS.FAILED
    try:
        fetch_log(job_server_log_query_url, project_id, component_id, task_id)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, error_code="LOG_FETCH_ERROR", detail=str(e))
        return state_final
    return state_final


@shared_task
@task_recorder
def robotx_spark_execute(project_id, component_id, task_id):
    from executor.components.AtomLearn import AtomLearn

    # 初始化 RobotxSpark类
    from executor.components.RobotXSpark import RobotXSpark
    component_type = extract_component_type(component_id)
    robotx_spark_class = eval(component_type)
    robotx_spark_obj = robotx_spark_class(project_id, component_id)

    # robotx 输出
    output_path, output_dict = robotx_spark_obj.output

    robotx_files = robotx_spark_obj.config_path
    config_name = RobotXSpark.CONFIG_FILE_NAME

    partial_command = common_setting(project_id, component_id)
    command = partial_command + [
               "--files", robotx_files,
               "--py-files", cluster_setting.ROBOTX_PY_FILES,
               cluster_setting.ROBOTX_RUN, config_name, output_path, "hive", output_dict, "n"]
    print(" ".join(command))
    try:
        p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cluster_setting.SPARK_ROBOTX_PATH)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, detail=str(e))
        return TASK_STATUS.FAILED

    status = spark_submit( project_id, component_id, task_id, p )
    if status != TASK_STATUS.SUCCEEDED:
        return status
    else:
        robotx_dict_local_path = AtomLearn.get_robotx_dict_local_path( project_id, component_id )
        robotx_dict_hdfs_path = AtomLearn.get_robotx_dict_hdfs_path( project_id, component_id )

        try:
            #下载生成字典
            download_report([robotx_dict_hdfs_path], [robotx_dict_local_path])
        except Exception as e:
            update_task_detail(project_id, component_id, task_id, error_code="REPORT_GENERATE_ERROR")
            return TASK_STATUS.FAILED
        # return spark_submit(project_id, component_id, task_id, p)
        return status


@shared_task
@task_recorder
def feature_combine_execute(project_id, component_id, task_id):
    from executor.components.FeatureCombine import FeatureCombine
    from executor.components.SelfDefinedFeature import SelfDefinedFeature
    from db_model.models import FeatureCombine as FeatureCombineModel

    feature_combines = FeatureCombineModel.objects.filter(project_id=project_id, component_id=component_id)
    feature_combine = feature_combines[0]
    connected_self_feature = feature_combine.self_defined_feature_id
    self_defined_csv_file = SelfDefinedFeature.csv_file_path(project_id, connected_self_feature)
    config_file_path = FeatureCombine.get_config_path(project_id, component_id)

    partial_command = common_setting(project_id, component_id)
    command = partial_command + [
               "--files", "%s,%s" %(self_defined_csv_file,config_file_path),
               cluster_setting.FEATURE_COMBINE_RUN, FeatureCombine.CONFIG_FILE_NAME]
    print(" ".join(command))
    try:
        p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         cwd=cluster_setting.FEATURE_COMBINE_PATH)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, detail=str(e))
        return TASK_STATUS.FAILED
    return spark_submit(project_id, component_id,task_id,p)

@shared_task
@task_recorder
def atom_learn_execute(project_id, component_id, task_id):
    from executor.components.AtomLearn import AtomLearn

    config_file_path = AtomLearn.get_config_path(project_id, component_id)
    hive_reader_dict_path = AtomLearn.hive_reader_dict_path(project_id, component_id)
    has_local_dict = os.path.exists(hive_reader_dict_path)

    partial_command = common_setting(project_id, component_id)
    command = partial_command + [
               "--files", ("%s,%s" % (config_file_path, hive_reader_dict_path)) if has_local_dict else config_file_path,
               "--py-files", cluster_setting.SPARK_ATOM_PY_FILES,
               cluster_setting.SPARK_ATOM_RUN, "Learn"
               ]
    print(" ".join(command))
    try:
        p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             cwd=cluster_setting.SPARK_ATOM_PATH)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, detail=str(e))
        return TASK_STATUS.FAILED
    status = spark_submit(project_id, component_id, task_id, p)
    if status != TASK_STATUS.SUCCEEDED:
        return status

    metrics_local = AtomLearn.get_model_metrics_local_path(project_id, component_id)
    metrics_hdfs = AtomLearn.get_model_metrics_hdfs_path(project_id, component_id)
    properties_local = AtomLearn.get_model_properties_local_path(project_id, component_id)
    properties_hdfs = AtomLearn.get_model_properties_hdfs_path(project_id, component_id)
    export_model_conf_local = AtomLearn.get_export_model_config_local_path(project_id, component_id)
    export_model_conf_hdfs = AtomLearn.get_export_model_config_hdfs_path(project_id, component_id)
    export_model_local = AtomLearn.get_export_model_local_path(project_id, component_id)

    try:
        #下载报告包括下载导出模型的配置文件
        download_report([metrics_hdfs, properties_hdfs, export_model_conf_hdfs], [metrics_local, properties_local,
                                                                                    export_model_conf_local])
        AtomLearn.generate_report(project_id, component_id)

        #下载导出模型文件
        export_model_hdfs_path = AtomLearn.get_export_model_hdfs_path( project_id, component_id )
        download_report([export_model_hdfs_path], [export_model_local])

        #打包模型文件与jar包文件为zip包
        AtomLearn.zip_export_model( project_id, component_id )

    except Exception as e:
        update_task_detail(project_id, component_id, task_id, error_code="REPORT_GENERATE_ERROR")
        return TASK_STATUS.FAILED
    return status


@shared_task
@task_recorder
def atom_act_execute(project_id, component_id, task_id):
    from executor.components.AtomAct import AtomAct

    config_file_path = AtomAct.get_config_path(project_id, component_id)

    partial_command = common_setting(project_id, component_id)
    command = partial_command + [
               "--files", config_file_path,
               "--py-files", cluster_setting.SPARK_ATOM_PY_FILES,
               cluster_setting.SPARK_ATOM_RUN, "Act"
               ]
    print(" ".join(command))
    try:
        p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             cwd=cluster_setting.SPARK_ATOM_PATH)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, detail=str(e))
        return TASK_STATUS.FAILED
    status = spark_submit(project_id, component_id, task_id, p)

    if status != TASK_STATUS.SUCCEEDED:
        return status

    bin_local = AtomAct.get_prediction_bin_local_path(project_id, component_id)
    bin_hdfs = AtomAct.get_prediction_bin_hdfs_path(project_id, component_id)
    csv_local = AtomAct.get_prediction_csv_local_path(project_id, component_id)
    csv_hdfs = AtomAct.get_prediction_csv_hdfs_path(project_id, component_id)

    try:
        download_report([bin_hdfs, csv_hdfs], [bin_local, csv_local])
        AtomAct.generate_report(project_id, component_id)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, error_code="REPORT_GENERATE_ERROR")
        return TASK_STATUS.FAILED
    return status

@shared_task
@task_recorder
def atom_test_execute(project_id, component_id, task_id):
    from executor.components.AtomTest import AtomTest

    config_file_path = AtomTest.get_config_path(project_id, component_id)

    partial_command = common_setting(project_id, component_id)
    command = partial_command + [
               "--files", config_file_path,
               "--py-files", cluster_setting.SPARK_ATOM_PY_FILES,
               cluster_setting.SPARK_ATOM_RUN, "Test"
               ]
    print(" ".join(command))
    try:
        p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             cwd=cluster_setting.SPARK_ATOM_PATH)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, detail=str(e))
        return TASK_STATUS.FAILED
    status = spark_submit(project_id, component_id, task_id, p)

    if status != TASK_STATUS.SUCCEEDED:
        return status

    metric_local = AtomTest.get_test_metrics_local_path(project_id, component_id)
    metric_hdfs = AtomTest.get_test_metrics_hdfs_path(project_id, component_id)

    try:
        download_report([metric_hdfs], [metric_local])
        AtomTest.generate_report(project_id, component_id)
    except Exception as e:
        update_task_detail(project_id, component_id, task_id, error_code="REPORT_GENERATE_ERROR")
        return TASK_STATUS.FAILED
    return status



