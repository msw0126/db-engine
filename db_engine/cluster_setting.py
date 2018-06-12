import os, sys
from F_SETTING import CLUSTER_COMPONENT_DIR, \
    PYSPARK_PYTHON, \
    DEFAULT_NUM_EXECUTORS, \
    DEFAULT_EXECUTOR_MEMORY, \
    DEFAULT_DRIVER_MEMORY, \
    DEFAULT_EXECUTOR_CORES, \
    DEFAULT_DRIVER_PERM, \
    DEFAULT_EXECUTOR_PERM,\
    CLUSTER_DIRECTORY, \
    YARN_IP, \
    CLUSTER_WORKING_DIRECTORY, \
    YARN_APPLICATION_URL, \
    JOB_SERVER_LOG_URL, \
    APPLICATION_ATTEMPT_URL, \
    APPLICATION_KILL_URL, \
    HADOOP_USER_NAME, \
    LOG_QUERY_PERIOD


# --------------QUEUE NAME NO NEED TO MODIFY---------------------#
QUEUE = "db-queue"
# --------------YARN CONFIG NO NEED TO MODIFY--------------------#
YARN_MODE = "yarn-cluster"
# --------------SPARK ROBOTX CONFIG NO NEED TO MODIFY------------#
ROBOTX_RUN = "robotx_run.py"
ROBOTX_PY_FILES = "robotx.zip,networkx.zip,decorator.py"
# --------------Feature combine Config NO NEED TO MODIFY---------#
FEATURE_COMBINE_RUN = "feature_combine.py"
# --------------SPARK ATOM CONFIG NO NEED TO MODIFY------------#
SPARK_ATOM_RUN = "Atom.py"
SPARK_ATOM_PY_FILES = "Atom.zip,backports.inspect-0.0.3.tar.gz,certifi.zip,chardet.zip,colorama.zip,future.zip,h2o_pysparkling_2.1-2.1.17.zip,idna.zip,pkg_resources.py,prettytable-0.7.2.zip,pytz.zip,requests.zip,tabulate.zip,traceback2-1.4.0.zip,urllib3-1.22.zip"

# -------------ATOM ROBOTX CONFIG------------------------------#
SPARK_PATH = os.path.join(CLUSTER_COMPONENT_DIR, "spark", "bin", "spark-submit")
SPARK_CLASSPATH = os.path.join(CLUSTER_COMPONENT_DIR, "spark", "jars")
HADOOP_CONFIG = os.path.join(CLUSTER_COMPONENT_DIR, "configuration")

# ---------------SPARK ATOM CONFIGERATION-------------
SPARK_ROBOTX_PATH = os.path.join(CLUSTER_COMPONENT_DIR, "RobotXSpark")
SPARK_ATOM_PATH = os.path.join(CLUSTER_COMPONENT_DIR, "AtomSpark")
FEATURE_COMBINE_PATH = os.path.join(CLUSTER_COMPONENT_DIR, "FeatureCombine")
