from typing import List

from django.http import HttpResponse

from common import ERRORS
from db_engine.hive_reader import StructureClass
from db_model.models import Container, SelfDefinedFeatureType, FeatureCombine, FeatureCombineRelation, IOFieldType, Task
from executor.components.FeatureCombine import FeatureCombine as FeatureCombineComp

from common.UTIL import auto_param, Response, py4j_common_hive_util
from executor import TASK_STATUS


class Connection:
    def __init__(self, robotx_field, self_defined_field):
        self.robotx_field = robotx_field
        self.self_defined_field = self_defined_field


@auto_param
def save_relation(request, project_id, component_id, robotx_spark_id,
                  self_defined_feature_id,
                  connections: List[Connection]):
    # 检查robotx
    objs = Container.objects.filter(project_id=project_id, component_id=robotx_spark_id)
    if len(objs) == 0:
        response = Response.fail(ERRORS.ROBOTX_NOT_CONFIGURED, None)
        return HttpResponse(response.to_json())

    connection_of_robotx = set()
    connection_of_self_defined = set()
    feature_combine_relations = list()
    for connection in connections:
        connection_of_robotx.add(connection.robotx_field)
        connection_of_self_defined.add(connection.self_defined_field)
        feature_combine_relations.append(
            FeatureCombineRelation(project_id=project_id,
                                   component_id=component_id,
                                   robotx_field=connection.robotx_field,
                                   self_defined_field=connection.self_defined_field)
        )

    # 检查连接字段是否在robotx中
    container = objs[0]
    table_name = container.table_name
    key_fields = set(container.key_fields.split(","))
    if not key_fields.issuperset(connection_of_robotx):
        response = Response.fail(ERRORS.FIELD_NOT_FOUND_IN_ROBOTX, None)
        return HttpResponse(response.to_json())

    # 检查 self_defined_feature
    objs = SelfDefinedFeatureType.objects.filter(field__in=connection_of_self_defined,
                                                 project_id=project_id, component_id=self_defined_feature_id)
    if len(objs) != len(connection_of_self_defined):
        response = Response.fail(ERRORS.FIELD_NOT_FOUND_IN_SELF_DEFINED, None)
        return HttpResponse(response.to_json())

    # 检查通过，保存
    FeatureCombine.objects.filter(project_id=project_id, component_id=component_id).delete()
    FeatureCombine(project_id=project_id, component_id=component_id, robotx_table_name=table_name,
                   robotx_spark_id=robotx_spark_id, self_defined_feature_id=self_defined_feature_id).save()

    FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id).delete()
    FeatureCombineRelation.objects.bulk_create(feature_combine_relations)
    return HttpResponse(Response.success().to_json())


@auto_param
def robotx_spark_key_fields(request, project_id, component_id):
    objs = Container.objects.filter(project_id=project_id, component_id=component_id)
    if len(objs) == 0:
        response = Response.fail(ERRORS.ROBOTX_NOT_CONFIGURED, None)
        return HttpResponse(response.to_json())

    container = objs[0]
    assert isinstance(container, Container)
    key_fields = container.key_fields.split(",")
    return HttpResponse(Response.success(key_fields).to_json())


@auto_param
def load_relation(request, project_id, component_id, robotx_spark_id, self_defined_feature_id):
    feature_combines = FeatureCombine.objects.filter(project_id=project_id, component_id=component_id)
    if len(feature_combines) == 0:
        # 以前没有配置过
        response = Response.success()
        return HttpResponse(response.to_json())

    data_changed = HttpResponse(Response.success("changed").to_json())

    # 检查组件是否变化
    feature_combine = feature_combines[0]
    assert isinstance(feature_combine, FeatureCombine)
    if feature_combine.robotx_spark_id != robotx_spark_id or \
                    feature_combine.self_defined_feature_id != self_defined_feature_id:
        return data_changed

    # 检查组件字段是否变化

    # 连接的robotx组件未配置
    objs = Container.objects.filter(project_id=project_id, component_id=robotx_spark_id)
    if len(objs) == 0:
        FeatureCombine.objects.filter(project_id=project_id, component_id=component_id).delete()
        FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id).delete()
        return data_changed

    feature_combine_relations = FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id)
    connection_of_robotx = set()
    connection_of_self_defined = set()
    connections = list()
    for feature_combine_relation in feature_combine_relations:
        assert isinstance(feature_combine_relation, FeatureCombineRelation)
        connection_of_robotx.add(feature_combine_relation.robotx_field)
        connection_of_self_defined.add(feature_combine_relation.self_defined_field)
        connections.append(
            Connection(feature_combine_relation.robotx_field, feature_combine_relation.self_defined_field))

    container = objs[0]
    key_fields = set(container.key_fields.split(","))
    if not key_fields.issuperset(connection_of_robotx):
        FeatureCombine.objects.filter(project_id=project_id, component_id=component_id).delete()
        FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id).delete()
        return data_changed

    # 检查 self_defined_feature
    objs = SelfDefinedFeatureType.objects.filter(field__in=connection_of_self_defined,
                                                 project_id=project_id, component_id=self_defined_feature_id)
    if len(objs) != len(connection_of_self_defined):
        FeatureCombine.objects.filter(project_id=project_id, component_id=component_id).delete()
        FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id).delete()
        return data_changed

    return HttpResponse(Response.success(connections).to_json())


container_fields_sql_lst = [
    "select a.*",
    "from db_model_iofieldtype a",
    "	INNER JOIN db_model_container b on a.project_id = b.project_id and a.component_id = b.container_id",
    "	INNER JOIN db_model_featurecombine c on b.project_id = c.project_id and b.component_id = c.robotx_spark_id",
    "where a.project_id = '{project_id}'",
    "	and c.component_id = '{component_id}'"
]

container_fields_sql = "\n".join(container_fields_sql_lst)


@auto_param
def container_fields(request, project_id, component_id):
    query_sql = container_fields_sql.format(
            project_id=project_id,
            component_id=component_id
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
    feature_combine_task = Task.objects.filter(project_id=project_id, component_id=component_id)
    if len(feature_combine_task)==0:
        return HttpResponse(Response.fail(ERRORS.FEATURE_COMBINE_NOT_SUCCESS).to_json())
    feature_combine_task = feature_combine_task[0]
    assert isinstance(feature_combine_task, Task)
    if feature_combine_task.task_status != TASK_STATUS.SUCCEEDED:
        return HttpResponse(Response.fail(ERRORS.FEATURE_COMBINE_NOT_SUCCESS).to_json())

    result_table = FeatureCombineComp.output_table(project_id, component_id)
    result = py4j_common_hive_util('viewTable', result_table, 10)
    if isinstance(result, HttpResponse):
        return result
    return HttpResponse(Response.success([dict(name=k,value=list(v)) for k,v in result.items()]).to_json())
