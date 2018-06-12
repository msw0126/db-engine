from io import BytesIO

from django.http import HttpResponse
from py4j.java_gateway import JavaGateway
from reportlab.platypus import SimpleDocTemplate

from common import ERRORS
from common.UTIL import auto_param, Response, COMPONENTS
from collections import OrderedDict

from common.report import Report
from db_engine import algorithms
from db_model.models import AtomLearn, AtomLearnParam, IOFieldType, ModelTopnMetricList, ModelScoreGroupThreshold
from db_engine.robotx_spark import container_query_sql_lst as robotx_container_lst
from db_engine.feature_combine import container_fields_sql_lst as combine_container_lst
from typing import List
import copy
from db_model.models import \
    ModelDescription, \
    ModelSummary, \
    ModelBestParams, \
    ModelCoefficient, \
    ModelVariableImportance, \
    ModelSyntheticMetrics, \
    ModelKFoldsSummary, \
    ModelMaxCriteria, \
    ModelGainLiftSummary, \
    ModelConfusionMatrix, \
    ModelThresholdsMetric

from django.db.models.query_utils import DeferredAttribute

from executor.celery_tasks import download_report


class ModelClassDescription:

    EXCLUDED = ['id', 'project_id', 'component_id']

    def __init__(self, cls):
        self.cls = cls
        self.name = cls.__name__
        self.props = list()
        self.__add_prop_()

    def __add_prop_(self):
        for prop, tp in self.cls.__dict__.items():
            if (not isinstance(tp, DeferredAttribute)) or prop in self.EXCLUDED:
                continue
            self.props.append(prop)


MODEL_OBJECTS = list()
MODEL_OBJECTS.append(ModelClassDescription(ModelDescription))
MODEL_OBJECTS.append(ModelClassDescription(ModelSummary))
MODEL_OBJECTS.append(ModelClassDescription(ModelBestParams))
MODEL_OBJECTS.append(ModelClassDescription(ModelCoefficient))
MODEL_OBJECTS.append(ModelClassDescription(ModelVariableImportance))
MODEL_OBJECTS.append(ModelClassDescription(ModelSyntheticMetrics))
MODEL_OBJECTS.append(ModelClassDescription(ModelKFoldsSummary))
MODEL_OBJECTS.append(ModelClassDescription(ModelGainLiftSummary))
MODEL_OBJECTS.append(ModelClassDescription(ModelConfusionMatrix))
MODEL_OBJECTS.append(ModelClassDescription(ModelMaxCriteria))
MODEL_OBJECTS.append(ModelClassDescription(ModelThresholdsMetric))
MODEL_OBJECTS.append(ModelClassDescription(ModelTopnMetricList))
MODEL_OBJECTS.append(ModelClassDescription(ModelScoreGroupThreshold))


def __orderd_dict__(params):

    odt = OrderedDict()
    for param in params:
        odt[param['name']] = param
    return odt


ALGORITHM_PARAMS = {
    algorithm: __orderd_dict__(params)
    for algorithm, params in algorithms.ALGORITHM_PARAMS.items()
}

field_in_sql = "and a.field in ('{id}','{target}')"
robotx_field_in_query = "\n".join(robotx_container_lst + [field_in_sql])
combine_field_in_query = "\n".join(combine_container_lst + [field_in_sql])

class Param(object):
    def __init__(self, name, values):
        self.name = name
        self.values = values


class ParamCheckingError(object):
    def __init__(self, name, error):
        self.name = name
        self.error = error


@auto_param
def save_with_default(request, project_id, atom_learn_id, input_comp_id, id, target, algorithm):
    """
    保存，算法的高级参数使用默认
    :param request:
    :param project_id:
    :param atom_learn_id:
    :param input_comp_id:
    :param id:
    :param target:
    :param algorithm:
    :return:
    """
    if algorithm not in ALGORITHM_PARAMS:
        return HttpResponse(Response.fail(ERRORS.ALGORITHM_NOT_SUPPORTED, None).to_json())
    AtomLearn.objects.update_or_create(
        project_id=project_id, component_id=atom_learn_id,
        defaults=dict(
            input_comp_id=input_comp_id, feature_id=id, feature_target=target, algorithm=algorithm))
    default_params = ALGORITHM_PARAMS[algorithm]
    params = list()
    for param in default_params:
        params.append(AtomLearnParam(project_id=project_id, component_id=atom_learn_id, param_name=param,
                                     param_value=str(default_params[param]['default'])))
    AtomLearnParam.objects.filter(project_id=project_id, component_id=atom_learn_id).delete()
    AtomLearnParam.objects.bulk_create(params)
    return HttpResponse(Response.success().to_json())


@auto_param
def save(request, project_id, atom_learn_id, input_comp_id, id, target, algorithm, params: List[Param]):
    if algorithm not in ALGORITHM_PARAMS:
        return HttpResponse(Response.fail(ERRORS.ALGORITHM_NOT_SUPPORTED, None).to_json())
    algorithm_params = ALGORITHM_PARAMS[algorithm]
    db_params = list()
    checking_results = list()
    for param in params:
        values = param.values
        param_name = param.name
        # 参数检查
        param_limit = algorithm_params[param_name]
        checking_result = param_checking(param_name, values, param_limit)
        if checking_result is not None:
            checking_results.append(checking_result)
        else:
            db_params.append(AtomLearnParam(project_id=project_id, component_id=atom_learn_id, param_name=param_name,
                                         param_value=values))
    # 参数有错
    if len(checking_results) > 0:
        return HttpResponse(Response.fail(ERRORS.ALGORITHM_PARAM_ERROR, checking_results).to_json())
    AtomLearn.objects.update_or_create(
        project_id=project_id, component_id=atom_learn_id,
        defaults=dict(
            input_comp_id=input_comp_id, feature_id=id, feature_target=target, algorithm=algorithm))
    AtomLearnParam.objects.filter(project_id=project_id, component_id=atom_learn_id).delete()
    AtomLearnParam.objects.bulk_create(db_params)
    return HttpResponse(Response.success().to_json())


@auto_param
def load(request, project_id, atom_learn_id, input_comp_id):
    atom_learn_db = AtomLearn.objects.filter(project_id=project_id, component_id=atom_learn_id)
    if len(atom_learn_db) == 0:
        # 刚新建组件
        return HttpResponse(Response.success().to_json())
    data_changed = HttpResponse(Response.success("changed").to_json())
    atom_learn = atom_learn_db[0]
    # 检查 input_comp_id 是否一样
    if atom_learn.input_comp_id != input_comp_id:
        atom_learn_db.delete()
        return data_changed
    # todo 检查 id， target 是否在其中，还缺少 robotx和自定义特征组合
    fields = list()
    if input_comp_id.startswith(COMPONENTS.HIVE_READER):
        # hive reader
        # hive reader 中是否包含这两个字段
        fields = IOFieldType.objects.filter(project_id=project_id, component_id=input_comp_id,
                                            field__in=[atom_learn.feature_id, atom_learn.feature_target])
    elif input_comp_id.startswith(COMPONENTS.ROBOTX_SPARK):
        # RobotXSpark
        fields = list(IOFieldType.objects.raw(robotx_field_in_query.format(
            project_id = project_id,
            component_id = input_comp_id,
            id = atom_learn.feature_id,
            target = atom_learn.feature_target
        )))
    elif input_comp_id.startswith(COMPONENTS.FEATURE_COMBINE):
        # feature combine
        fields = list(IOFieldType.objects.raw(combine_field_in_query.format(
            project_id=project_id,
            component_id=input_comp_id,
            id=atom_learn.feature_id,
            target=atom_learn.feature_target
        )))
    # id target 不在字段中
    if len(fields)!=2:
        atom_learn_db.delete()
        return data_changed

    # 检查通过，返回需要初始化的内容
    algorithm_params = ALGORITHM_PARAMS[atom_learn.algorithm]
    atom_learn_params = AtomLearnParam.objects.filter(project_id=project_id, component_id=atom_learn_id)
    params = list()
    for atom_learn_param in atom_learn_params:
        algorithm_param = copy.copy(algorithm_params[atom_learn_param.param_name])
        algorithm_param['value'] = atom_learn_param.param_value
        params.append(algorithm_param)
    result = dict(
        id = atom_learn.feature_id,
        target = atom_learn.feature_target,
        algorithm = atom_learn.algorithm,
        params = params
    )
    return HttpResponse(Response.success(result).to_json())


def param_checking(name, value, param_limit):
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value == "":
        return ParamCheckingError(name, ERRORS.EMPTY_PARAM)
    multiple = param_limit["multiple"]
    type_ = param_limit["type"]
    if not multiple:
        if type_ == "int":
            try:
                int(value)
            except Exception:
                return ParamCheckingError(name, ERRORS.VALUE_ERROR_PARAM)
        elif type_ == "double":
            try:
                float(value)
            except Exception:
                return ParamCheckingError(name, ERRORS.VALUE_ERROR_PARAM)
        elif type_ == "boolean":
            if value not in ["true","false"]:
                return ParamCheckingError(name, ERRORS.VALUE_ERROR_PARAM)
    else:
        values = value.split(",")
        for value in values:
            if type_ == "int":
                try:
                    int(value)
                except Exception:
                    return ParamCheckingError(name, ERRORS.VALUE_ERROR_PARAM)
            elif type_ == "double":
                try:
                    float(value)
                except Exception:
                    return ParamCheckingError(name, ERRORS.VALUE_ERROR_PARAM)

@auto_param
def report(request, project_id, component_id):
    res = dict()
    for model_obj in MODEL_OBJECTS:
        prop = model_obj.name
        values = model_obj.cls.objects.filter(project_id=project_id, component_id=component_id)
        if len(values) == 0:
            if model_obj == ModelDescription:
                break
            else:
                continue
        value_lst = list()
        for val in values:
            value_lst.append({
                p: val.__getattribute__(p)
                for p in model_obj.props
            })
        res[prop] = value_lst
    if len(res) == 0:
        return HttpResponse(Response.fail(ERRORS.NO_REPORT).to_json())
    return HttpResponse(Response.success(res).to_json())


@auto_param
def report_pdf(request, project_id, component_id, threshold_confusion, threshold_top):
    threshold_confusion = float(threshold_confusion)
    threshold_top = float(threshold_top)

    response = HttpResponse(content_type='application/pdf')
    response['Content-Disposition'] = 'attachment; filename=%s_%s.pdf' %( project_id, component_id)

    with BytesIO() as temp:
        doc = SimpleDocTemplate(temp)
        content = Report.learn_report(project_id, component_id, doc.width, threshold_confusion, threshold_top)
        doc.build(content)
        response.write(temp.getvalue())
    return response
