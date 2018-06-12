import json
import re

import requests
import time

from db_model.models import Task
from cluster_setting import YARN_APPLICATION_URL, JOB_SERVER_LOG_URL, APPLICATION_ATTEMPT_URL, LOG_QUERY_PERIOD
from executor import TASK_STATUS


def query(application_id, process_recorder, error_extractor):
    application_state_query_url = YARN_APPLICATION_URL.format(app_id=application_id)
    application_attempt_url = APPLICATION_ATTEMPT_URL.format(app_id=application_id)
    job_server_log_query_url = None

    app_node = None
    app_container = None
    app_user = None
    while True:
        print("[request]%s" % application_state_query_url)
        r = requests.get(application_state_query_url)
        if r.status_code == 404:
            print("no such application id")
        elif r.status_code == 200:
            app_info = json.loads(r.content.decode())['app']
            state_now = app_info['state']
            app_user = app_info['user']

            if state_now == TASK_STATUS.KILLED:
                break
            # 获取日志的node, container
            if app_node is None and state_now != "ACCEPTED":
                r = requests.get(application_attempt_url)
                attempt_info = json.loads(r.content.decode())['appAttempts']['appAttempt'][0]
                app_node = attempt_info['nodeId']
                if app_node == "":
                    app_node = None
                    continue
                app_container = attempt_info['containerId']
                job_server_log_query_url = JOB_SERVER_LOG_URL.format(
                    node=app_node,
                    container=app_container,
                    user=app_user
                )
                print("application attempt url: %s" % application_attempt_url)
                print("job_server_log_query_url: %s" % job_server_log_query_url)
            if state_now == TASK_STATUS.FAILED:
                r = requests.get(job_server_log_query_url)
                log_html = r.content.decode()
                std_out_with_html = re.split("Log Length: \d*", log_html)[-1]
                std_out = re.sub("<.*>", '', std_out_with_html).strip()
                if "Logs not available for" in std_out and "diagnostics" in app_info:
                    std_out = app_info['diagnostics']
                process_recorder.record(std_out)
                break
            if state_now == TASK_STATUS.FINISHED:
                state_now = app_info['finalStatus']
                if state_now == TASK_STATUS.SUCCEEDED:
                    break
                r = requests.get(job_server_log_query_url)
                log_html = r.content.decode()
                std_out_with_html = re.split("Log Length: \d*", log_html)[-1]
                std_out = re.sub("<.*>", '', std_out_with_html).strip()
                if "Logs not available for" in std_out and "diagnostics" in app_info:
                    std_out = app_info['diagnostics']
                error_extractor.record(std_out)
                break
            if state_now == TASK_STATUS.RUNNING:
                running_log_url = app_info['amContainerLogs'] + "/stdout"
                r = requests.get(running_log_url)
                log_html = r.content.decode()
                std_out_with_html = log_html.split("class=\"content\">")[-1]
                std_out = re.sub("<.*>", '', std_out_with_html).strip()
                process_recorder.record(std_out)
        time.sleep(LOG_QUERY_PERIOD)
    return state_now, job_server_log_query_url


class ProcessRecorder(object):
    def __init__(self, project_id, component_id, task_id, record_method):
        self.project_id = project_id
        self.component_id = component_id
        self.task_id = task_id
        self.record_method = record_method

    def record(self, x):
        self.record_method(self.project_id, self.component_id, self.task_id, detail=x)


class ErrorRecorder(object):
    def __init__(self, project_id, component_id, task_id, record_method, error_extractor=None):
        self.project_id = project_id
        self.component_id = component_id
        self.task_id = task_id
        self.record_method = record_method
        self.error_extractor = error_extractor

    def record(self, x):
        error_code = None
        if self.error_extractor is not None:
            self.error_extractor(x)
        self.record_method(self.project_id, self.component_id, self.task_id, error_code=error_code, detail=x)
