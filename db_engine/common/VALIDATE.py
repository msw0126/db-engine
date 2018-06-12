from common.UTIL import Response, COMPONENTS
from common import ERRORS


def project_id(id):
    try:
        int(id)
        return None
    except:
        return 'project_id %s is not a number' % id


def not_null_validate(v, name):
    if v is None or v.strip() == '':
        return '%s is empty' % name


COMPONENT_TYPES = set(
    [value for key, value in COMPONENTS.__dict__.items() if not key.startswith("__")])


def component_type(tp):
    if tp not in COMPONENT_TYPES:
        return '%s is not a valid component type' % tp


def component_id_validate(id: str, prefix):
    if not id.startswith(prefix):
        return Response.fail(ERRORS.COMPONENT_ID_ERROR,
                             '%s not start with %s' % (id, prefix))


def chain_validate(validators, params):
    messages = []
    for validator, param in zip(validators, params):
        res = validator(param)
        if res is not None:
            messages.append(res)
    if len(messages) == 0:
        return None
    return Response.fail(ERRORS.PARAMETER_VALUE_ERROR, "\n".join(messages))
