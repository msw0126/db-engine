from typing import List

from django.http import HttpResponse

from common.UTIL import auto_param, Response
from db_engine.hive_reader import StructureClass
from db_model.models import IOFieldType


@auto_param
def save_field_type(request, project_id, component_id, structures: List[StructureClass]):
    field_types = list()
    IOFieldType.objects.filter(project_id=project_id, component_id=component_id).delete()
    for structure in structures:
        field_type = IOFieldType(project_id = project_id,
                                 component_id = component_id,
                                 field = structure.field,
                                 field_type = structure.field_type,
                                 database_type = structure.database_type,
                                 date_format = structure.date_format,
                                 date_size= structure.date_size,
                                 ignore=structure.ignore,
                                 selected=structure.selected
                                 )
        field_types.append(field_type)
    IOFieldType.objects.bulk_create(field_types)
    return HttpResponse(Response.success(None).to_json())


@auto_param
def load_field_type(request, project_id, component_id, avaliable=False):
    if avaliable:
        field_type = IOFieldType.objects.filter(project_id=project_id,
                                                component_id=component_id,
                                                ignore = False,
                                                selected = True
                                                )
    else:
        field_types = IOFieldType.objects.filter(project_id=project_id, component_id=component_id)
    structures = []
    for field_type in field_types:
        structure = StructureClass(field_type.field, field_type.field_type,
                                   field_type.database_type, field_type.date_format,
                                   field_type.date_size, field_type.ignore, field_type.selected)
        structures.append(structure)
    return HttpResponse(Response.success(structures).to_json())
