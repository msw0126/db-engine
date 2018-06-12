from django.http import HttpResponse
from common.UTIL import auto_param, Response
from cluster_setting import DEFAULT_DRIVER_MEMORY, DEFAULT_NUM_EXECUTORS, \
    DEFAULT_EXECUTOR_MEMORY,DEFAULT_EXECUTOR_CORES, \
    DEFAULT_DRIVER_PERM, DEFAULT_EXECUTOR_PERM
from db_model.models import YarnResource as YarnResourceModel


class YarnResource:

    def __init__(self, driver_memory=DEFAULT_DRIVER_MEMORY,
                 num_executor=DEFAULT_NUM_EXECUTORS,
                 executor_memory=DEFAULT_EXECUTOR_MEMORY,
                 executor_cores=DEFAULT_EXECUTOR_CORES,
                 driver_perm = DEFAULT_DRIVER_PERM,
                 executor_perm = DEFAULT_EXECUTOR_PERM
                 ):
        self.driver_memory = driver_memory
        self.num_executor = num_executor
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_perm = driver_perm
        self.executor_perm = executor_perm


def yarn_resource(project_id, component_id):
    yarn_model = YarnResourceModel.objects.filter(project_id=project_id, component_id=component_id)
    if len(yarn_model) == 0:
        return YarnResource()
    yarn_model = yarn_model[0]
    return YarnResource(
        yarn_model.driver_memory,
        yarn_model.num_executors,
        yarn_model.executor_memory,
        yarn_model.executor_cores,
        yarn_model.driver_perm,
        yarn_model.executor_perm
    )

DEFAULT_YARN_RESOURCE_RESPONSE = HttpResponse(Response.success(YarnResource()).to_json())


@auto_param
def previous(request, project_id, component_id):
    yarn_model = YarnResourceModel.objects.filter(project_id=project_id, component_id=component_id)
    if len(yarn_model) == 0:
        return DEFAULT_YARN_RESOURCE_RESPONSE
    yarn_model = yarn_model[0]
    assert isinstance(yarn_model, YarnResourceModel)
    return HttpResponse(Response.success(YarnResource(
        yarn_model.driver_memory,
        yarn_model.num_executors,
        yarn_model.executor_memory,
        yarn_model.executor_cores,
        yarn_model.driver_perm,
        yarn_model.executor_perm
    )).to_json())


@auto_param
def save(request, project_id, component_id, driver_memory,
         num_executor, executor_memory, executor_cores,
         driver_perm, executor_perm):
    YarnResourceModel.objects.update_or_create(
        project_id=project_id, component_id=component_id,
        defaults=dict(
            driver_memory=driver_memory,
            num_executors=num_executor,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            driver_perm=driver_perm,
            executor_perm=executor_perm
        ))
    return HttpResponse(Response.success().to_json())
