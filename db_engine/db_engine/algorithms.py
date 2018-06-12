from django.http import HttpResponse
from common import ERRORS
from common.UTIL import auto_param, Response
import setting
import copy

ALGORITHMS = [dict(
    name = algorithm['name'],
    full_name = algorithm['full_name'],
    chinese = algorithm['chinese'],
    description = algorithm['description']
) for algorithm in setting.ALGORITHMS]

ALGORITHM_PARAMS = {
    algorithm['name'] : copy.copy(setting.COMMON_PARAMS) + algorithm['params']
    for algorithm in setting.ALGORITHMS
}


@auto_param
def list(request):
    return HttpResponse(Response.success(ALGORITHMS).to_json())


@auto_param
def list_params(request, algorithm):
    if algorithm not in ALGORITHM_PARAMS:
        return HttpResponse(Response.fail(ERRORS.ALGORITHM_NOT_SUPPORTED, None).to_json())
    return HttpResponse(Response.success(ALGORITHM_PARAMS[algorithm]).to_json())
