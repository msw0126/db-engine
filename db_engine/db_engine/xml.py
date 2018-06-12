from common.UTIL import auto_param, Response, mk_working_directory
import os
from django.http import HttpResponse
from setting import WORKING_DIRECTORY


def save_xml(project_id, xml):
    saving_path = os.path.join(WORKING_DIRECTORY, project_id)
    if not os.path.exists(saving_path) or not os.path.isdir(saving_path):
        os.makedirs(saving_path)
    with open(os.path.join(saving_path, "config.xml"), 'w') as f:
        f.write(xml)

@auto_param
def save(request, project_id, xml):
    save_xml(project_id, xml)
    return HttpResponse(Response.success().to_json())


@auto_param
def load(request, project_id):
    config_path = os.path.join(WORKING_DIRECTORY, project_id, "config.xml")
    if not os.path.exists(config_path):
        return HttpResponse(Response.success('').to_json())

    with open(config_path, 'r') as f:
        xml = "".join(f.readlines())
        return HttpResponse(Response.success(xml).to_json())


