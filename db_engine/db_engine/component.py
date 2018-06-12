from db_model.models import CompIDGenerator, Task, FeatureCombineRelation, AtomLearnParam, YarnResource, ExportModel
from db_model.models import FeatureCombine as FeatureCombineModel
from db_model.models import AtomLearn as AtomLearnModel
from db_model.models import AtomAct as AtomActModel
from db_model.models import AtomTest as AtomTestModel
from common.UTIL import auto_param, Response, COMPONENTS, py4j_common_hive_util, extract_component_type, \
    mk_working_directory, del_working_directory
from common import VALIDATE, ERRORS
from django.http import HttpResponse
from db_model.models import HiveReader, IOFieldType, SelfDefinedFeature, SelfDefinedFeatureType, Container, Relation
from executor.components.RobotXSpark import RobotXSpark
from executor.components.FeatureCombine import FeatureCombine
from executor.components.AtomLearn import AtomLearn
from executor.components.AtomAct import AtomAct
from executor.components.AtomTest import AtomTest
from executor.components.Component import Component


@auto_param
def get_id(request, project_id, component_type):
    # 检查数据类型是否正确
    validate_result = VALIDATE.chain_validate([VALIDATE.project_id, VALIDATE.component_type]
                                              , [project_id, component_type])
    if validate_result is not None:
        return HttpResponse(validate_result.to_json())
    # 查询数据库中是否有project_id
    query_result = CompIDGenerator.objects.filter(project_id=project_id)
    if len(query_result)==0:
        # 没有，component_id返回0，并保存记录到数据库
        CompIDGenerator(project_id=project_id, component_id=1).save()
        resp = Response.success('%s%d' %(component_type, 0))
        return HttpResponse(resp.to_json())
    else:
        # 有，component_id +1 ，返回，并更新数据库
        comp_id_generator = query_result[0]
        component_id = comp_id_generator.component_id
        comp_id_generator.component_id = comp_id_generator.component_id+1
        comp_id_generator.save()
        resp = Response.success('%s%d' % (component_type, component_id))
        return HttpResponse(resp.to_json())


@auto_param
def save_hive_reader(request, project_id, component_id, table_name, logic_name):
    # 检查数据类型是否正确
    # project_id是否为数字
    # component_id是否以HiverReader开头+数字
    # table_name在数据库中是否存在
    # logic_name 是否符合规范
    component_id_validate = VALIDATE.component_id_validate(component_id, COMPONENTS.HIVE_READER)
    if component_id_validate is not None:
        return HttpResponse(component_id_validate.to_json())

    result = py4j_common_hive_util('checkExist', table_name)
    if isinstance(result, HttpResponse):
        return result
    if not result:
        return HttpResponse(Response.fail(ERRORS.HIVE_TABLE_NOT_EXIST, None).to_json())

    HiveReader.objects.filter(project_id=project_id, component_id=component_id).delete()
    HiveReader(project_id=project_id, component_id=component_id,
               table_name=table_name, logic_name= logic_name).save()
    return HttpResponse(Response.success(None).to_json())


@auto_param
def load_hive_reader(request, project_id, component_id):
    component_id_validate = VALIDATE.component_id_validate(component_id, COMPONENTS.HIVE_READER)
    if component_id_validate is not None:
        return HttpResponse(component_id_validate.to_json())
    hive_readers = HiveReader.objects.filter(project_id=project_id, component_id=component_id)
    if len(hive_readers)==0:
        # 组件不存在
        response = Response.success()
        return HttpResponse(response.to_json())
    hive_reader = hive_readers[0]
    response = Response.success(dict(
        table_name = hive_reader.table_name,
        logic_name = hive_reader.logic_name
    ))
    return HttpResponse(response.to_json())


DB_DEL = {
    COMPONENTS.HIVE_READER : (HiveReader, IOFieldType),
    COMPONENTS.SELF_DEFINED_FEATURE : (SelfDefinedFeature, SelfDefinedFeatureType),
    COMPONENTS.ROBOTX_SPARK : (Container,Relation),
    COMPONENTS.FEATURE_COMBINE : (FeatureCombineModel, FeatureCombineRelation),
    COMPONENTS.ATOM_LEARN : (AtomLearnModel, AtomLearnParam),
    COMPONENTS.ATOM_ACT : (AtomActModel,),
    COMPONENTS.ATOM_TEST: (AtomTestModel,),
    COMPONENTS.EXPORT_MODEL: (ExportModel,)
}

EXECUTABLE = {
    COMPONENTS.ROBOTX_SPARK : RobotXSpark.output_table,
    COMPONENTS.FEATURE_COMBINE : FeatureCombine.output_table,
    COMPONENTS.ATOM_LEARN : lambda p,c: None,
    COMPONENTS.ATOM_ACT : lambda p,c: None,
    COMPONENTS.ATOM_TEST : lambda p,c: None
}


@auto_param
def delete(request, project_id, component_id):

    comp_type = extract_component_type(component_id)
    del_dbs = DB_DEL[comp_type]
    # 删除数据库
    db_del(project_id,component_id, *del_dbs)
    # 删除工作目录
    del_working_directory(project_id, component_id)

    if comp_type in EXECUTABLE:
        # 删除集群产出
        table = EXECUTABLE[comp_type](project_id, component_id)
        common_del(project_id, component_id, table)
    return HttpResponse(Response.success().to_json())


def db_del(project_id, component_id,*clss):
    for cls in clss:
        cls.objects.filter(project_id=project_id, component_id=component_id).delete()


def common_del(project_id, component_id, table):
    Task.objects.filter(project_id=project_id, component_id=component_id).delete()
    YarnResource.objects.filter(project_id=project_id, component_id=component_id).delete()
    # delete hdfs working directory
    cluster_working_dir = Component.cluster_working_directory(project_id, component_id)
    py4j_common_hive_util('cleanComponent', cluster_working_dir, table)
