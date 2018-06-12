import codecs
import csv
import os

from common.UTIL import auto_param, Response, COMPONENTS, py4j_common_hive_util
from common import VALIDATE, ERRORS
from django.http import HttpResponse, StreamingHttpResponse
from db_model.models import Container, Relation, IOFieldType, HiveReader, Task
from db_engine.hive_reader import StructureClass
from typing import List

from executor import TASK_STATUS
from executor.components.RobotXSpark import RobotXSpark
from setting import WORKING_DIRECTORY
from setting import WORKING_DIRECTORY
from F_SETTING import CLUSTER_DIRECTORY


class Join:
    def __init__(self, sc_field, tg_field):
        self.sc_field = sc_field
        self.tg_field = tg_field


@auto_param
def save_container(request, project_id, component_id, table_name, container_id, key_fields: List[str]):
    # 检查robotx id是否合法
    check_robotx = VALIDATE.component_id_validate(component_id, COMPONENTS.ROBOTX_SPARK)
    if check_robotx is not None:
        return HttpResponse(check_robotx.to_json())
    # 检查container id 是否合法
    check_container = VALIDATE.component_id_validate(component_id, COMPONENTS.HIVE_READER)
    if check_robotx is not None:
        return HttpResponse(check_container.to_json())

    container_pre = Container.objects.filter(project_id=project_id, component_id=component_id)
    if len(container_pre) > 0:
        pre_container = container_pre[0]
        if pre_container.container_id != container_id \
            or set(pre_container.key_fields.split(",")) != set(key_fields):
            Relation.objects.filter(project_id=project_id, component_id=component_id).delete()
        container_pre.delete()
    Container(project_id=project_id, component_id=component_id, table_name=table_name, container_id=container_id,
              key_fields=",".join(key_fields)).save()
    return HttpResponse(Response.success().to_json())


@auto_param
def save_relation(request, project_id, component_id, source, source_table_name, target, target_table_name,
                  join: List[Join], rel_type, interval = None):
    check_result = check_relation(component_id, source, target)
    if check_result is not None:
        return check_result

    Relation.objects.filter(project_id=project_id, component_id=component_id, source=source, target=target).delete()
    Relation(project_id=project_id, component_id=component_id, source=source, target=target,
             source_table_name=source_table_name,
             target_table_name=target_table_name,
             sc_join=",".join([v.sc_field for v in join]),
             tg_join=",".join([v.tg_field for v in join]),
             rel_type=rel_type, interval=interval).save()
    return HttpResponse(Response.success().to_json())


def check_relation(component_id, source, target):
    # 检查robotx id是否合法
    check_robotx = VALIDATE.component_id_validate(component_id, COMPONENTS.ROBOTX_SPARK)
    if check_robotx is not None:
        return HttpResponse(check_robotx.to_json())
    # 检查source 是否合法
    check_source = VALIDATE.component_id_validate(source, COMPONENTS.HIVE_READER)
    if check_source is not None:
        return HttpResponse(check_source.to_json())
    # 检查target 是否合法
    check_target = VALIDATE.component_id_validate(target, COMPONENTS.HIVE_READER)
    if check_target is not None:
        return HttpResponse(check_target.to_json())


@auto_param
def delete_relation(request, project_id, component_id, source, target):
    check_result = check_relation(component_id, source, target)
    if check_result is not None:
        return check_result

    Relation.objects.filter(project_id=project_id, component_id=component_id, source=source, target=target).delete()
    return HttpResponse(Response.success().to_json())


@auto_param
def check_configuration(request, project_id, component_id, inputs: List[str]):
    data_changed = HttpResponse(Response.success("changed").to_json())

    fields_map = dict()
    containers = Container.objects.filter(project_id=project_id, component_id=component_id)
    if len(containers) == 0:
        return HttpResponse(Response.success().to_json())

    container = containers[0]
    fields_map[container.container_id] = set(container.key_fields.split(","))
    # 检查 container_id 是否在 inputs 中
    if container.container_id not in inputs:
        containers.delete()
        Relation.objects.filter(project_id=project_id, component_id=component_id).delete()
        return data_changed
    # 检查表名
    container_table_name_check = HiveReader.objects.filter(project_id=project_id, component_id=container.container_id,
                                                           table_name=container.table_name)
    if len(container_table_name_check) == 0:
        containers.delete()
        Relation.objects.filter(project_id=project_id, component_id=component_id).delete()
        return data_changed
    # 检查所有关系的source target 是否在 inputs中
    relations = Relation.objects.filter(project_id=project_id, component_id=component_id)
    relation_mess = []
    for relation in relations:
        if relation.source not in inputs:
            relations.delete()
            return data_changed
        relation_table_check = HiveReader.objects.filter(project_id=project_id, component_id=relation.source,
                                                         table_name=relation.source_table_name)
        if len(relation_table_check) == 0:
            relations.delete()
            return data_changed
        # 构造在关系中，出现的字段，用于判断，数据是否变更
        sc_field = relation.sc_join.split(",")
        if relation.source in fields_map:
            fields_map[relation.source] |= set(sc_field)
        else:
            fields_map[relation.source] = set(sc_field)
        tg_field = relation.tg_join.split(",")
        if relation.target in fields_map:
            fields_map[relation.target] |= set(tg_field)
        else:
            fields_map[relation.target] = set(tg_field)
        relation_mess.append(dict(
            source=relation.source,
            target=relation.target,
            rel_type=relation.rel_type,
            interval=relation.interval,
            join=[
                {
                    'sc_field': sc,
                    'tg_field': tg
                }
                for sc, tg in zip(sc_field, tg_field)
            ]
        ))
    # 检查container记录的key_fields,relation中的字段 是否在对应表的字段中
    for comp_id, fields in fields_map.items():
        if not field_in_table(fields, project_id, comp_id):
            containers.delete()
            Relation.objects.filter(project_id=project_id, component_id=component_id).delete()
            return data_changed

    container_mess = dict(
        container_id=container.container_id,
        key_fields=container.key_fields.split(",")
    )
    return HttpResponse(Response.success(dict(
        container=container_mess,
        relations=relation_mess
    )).to_json())


def field_in_table(fields, project_id, component_id):
    table_fields = IOFieldType.objects.only('field').filter(project_id=project_id, component_id=component_id,
                                                            ignore=False)
    table_fields_set = set()
    for table_field in table_fields:
        table_fields_set.add(table_field.field)
    return table_fields_set >= fields


@auto_param
def save_xml(request, project_id, component_id, xml):
    saving_path = os.path.join(WORKING_DIRECTORY, project_id, component_id)
    if not os.path.exists(saving_path) or not os.path.isdir(saving_path):
        os.makedirs(saving_path)
    with open(os.path.join(saving_path, "RobotXSpark.xml"), 'w') as f:
        f.write(xml)
    return HttpResponse(Response.success().to_json())


@auto_param
def load_xml(request, project_id, component_id):
    config_path = os.path.join(WORKING_DIRECTORY, project_id, component_id, "RobotXSpark.xml")
    if not os.path.exists(config_path):
        return HttpResponse(Response.success('').to_json())

    with open(config_path, 'r') as f:
        xml = "".join(f.readlines())
        return HttpResponse(Response.success(xml).to_json())


container_query_sql_lst = [
    "select a.*",
    "from db_model_iofieldtype a",
    "	INNER JOIN db_model_container b on a.project_id = b.project_id and a.component_id = b.container_id",
    "where a.project_id = '{project_id}'",
    "	and b.component_id = '{component_id}'"
]

container_query_sql = "\n".join(container_query_sql_lst)


@auto_param
def container_fields(request, project_id, component_id):
    query_sql = container_query_sql.format(
        project_id = project_id,
        component_id = component_id
    )
    field_types = list(IOFieldType.objects.raw(query_sql))
    if len(field_types) == 0:
        return HttpResponse(Response.success().to_json())
    structures = []
    for field_type in field_types:
        structure = StructureClass(field_type.field, field_type.field_type,
                                   field_type.database_type, field_type.date_format, field_type.date_size,
                                   field_type.ignore)
        structures.append(structure)
    return HttpResponse(Response.success(structures).to_json())


@auto_param
def view_table(request, project_id, component_id):
    robotx_task = Task.objects.filter(project_id=project_id, component_id=component_id)
    if len(robotx_task)==0:
        return HttpResponse(Response.fail(ERRORS.ROBOTX_NOT_SUCCESS).to_json())
    robotx_task = robotx_task[0]
    assert isinstance(robotx_task, Task)
    if robotx_task.task_status != TASK_STATUS.SUCCEEDED:
        return HttpResponse(Response.fail(ERRORS.ROBOTX_NOT_SUCCESS).to_json())

    result_table = RobotXSpark.output_table(project_id, component_id)
    result = py4j_common_hive_util('viewTable', result_table, 10)
    if isinstance(result, HttpResponse):
        return result
    return HttpResponse(Response.success([dict(name=k,value=list(v)) for k,v in result.items()]).to_json())


@auto_param
def download_dict(request, project_id, component_id):
    response = HttpResponse(content_type='text/csv')
    response.write(codecs.BOM_UTF8)
    file_name = os.path.join(WORKING_DIRECTORY, project_id, component_id, "dict.csv")
    response['Content-Disposition'] = 'attachment;filename=%s_%s.csv' % ( project_id, component_id)
    writer = csv.writer(response)
    csv_reader = csv.reader(open(file_name, encoding='utf-8'))
    for row in csv_reader:
        writer.writerow(row)
    return response




