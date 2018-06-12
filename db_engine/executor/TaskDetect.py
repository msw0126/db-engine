from common.UTIL import extract_component_type
from db_model.models import Task,Execution
from executor.TASK_STATUS import PENDING,ExecutionStatus
from executor.components.FeatureCombine import FeatureCombine
from executor.components.RobotXSpark import RobotXSpark
from executor.components.AtomLearn import AtomLearn
from executor.components.AtomAct import AtomAct
from executor.components.AtomTest import AtomTest
import logging

logger = logging.getLogger("monitor")


def task_detect():
    executions = Execution.objects.filter(status=ExecutionStatus.RUNNING) # type: list[Execution]
    executions = [ec.task_id for ec in executions]
    tasks = Task.objects.filter(task_status=PENDING, relies=0, task_id__in=executions)
    for task in tasks:
        project_id = task.project_id
        component_id = task.component_id
        task_id = task.task_id
        component_type = extract_component_type(component_id)
        executor_class = eval(component_type)
        executor = executor_class(project_id, component_id)
        executor.execute(task_id)
        logger.info("%s-%s-%s submitted to task queue" %(project_id, component_id, task_id))
