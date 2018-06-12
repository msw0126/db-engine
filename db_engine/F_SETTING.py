LOG_DIR = "/app/taoshu/tzb_databrain/engine/db_engine/log/"
WORKING_DIRECTORY = "/app/taoshu/tzb_databrain/engine/work_dir"
FONT_DIR = "/app/taoshu/tzb_databrain/engine/db_engine/common/report"

HIVE_OUTPUT_DB= "taoshu_db_output"
HIVE_INPUT_DB = "taoshu_db_input"

DB_ENGINE = 'django.db.backends.mysql'
DB_NAME = 'engine'
DB_USER = 'root'
DB_PASSWORD = 'taoshu12345'
DB_HOST = 'localhost'
DB_PORT = '3306'

CLUSTER_COMPONENT_DIR = "/app/taoshu/tzb_databrain/engine/cluster_components"
# ---------------python config------------------------
# hadoop集群已经不需要安装python2.7，赋值为None就可以。
PYSPARK_PYTHON = None

# ---------------yarn resource -----------------------
DEFAULT_NUM_EXECUTORS = 8
DEFAULT_EXECUTOR_MEMORY = 1 # in GB
DEFAULT_DRIVER_MEMORY = 4 # in GB
DEFAULT_EXECUTOR_CORES = 1
DEFAULT_DRIVER_PERM = 128
DEFAULT_EXECUTOR_PERM = 128

# YARN 工作目录及log查询相关
CLUSTER_DIRECTORY = "/taoshu/engine/work_dir"


YARN_IP = "node1" # yarn resource manager host or ip
CLUSTER_WORKING_DIRECTORY = "hdfs://%s:8020%s" %(YARN_IP, CLUSTER_DIRECTORY)
YARN_APPLICATION_URL = "http://%s:8088/ws/v1/cluster/apps/{app_id}" %YARN_IP
JOB_SERVER_LOG_URL = "http://%s:19888/jobhistory/logs/{node}/{container}/{container}/{user}" %YARN_IP
APPLICATION_ATTEMPT_URL = YARN_APPLICATION_URL + "/appattempts"
APPLICATION_KILL_URL = "http://%s:8088/ws/v1/cluster/apps/{app_id}/state" %YARN_IP
HADOOP_USER_NAME = "hdfs"
LOG_QUERY_PERIOD = 10 # in seconds
