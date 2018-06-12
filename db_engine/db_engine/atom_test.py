from django.http import HttpResponse
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate

from common import ERRORS
from common.UTIL import auto_param, Response, COMPONENTS,extract_component_type
from common.report import Report
from db_model.models import AtomLearn, IOFieldType, AtomTest, ModelSyntheticMetrics, ModelGainLiftSummary, \
    ModelConfusionMatrix, ModelMaxCriteria, ModelThresholdsMetric, ModelTopnMetricList, ModelScoreGroupThreshold
from db_engine.atom_learn import robotx_field_in_query, combine_field_in_query, ModelClassDescription

MODEL_OBJECTS = list()
MODEL_OBJECTS.append(ModelClassDescription(ModelSyntheticMetrics))
MODEL_OBJECTS.append(ModelClassDescription(ModelGainLiftSummary))
MODEL_OBJECTS.append(ModelClassDescription(ModelConfusionMatrix))
MODEL_OBJECTS.append(ModelClassDescription(ModelMaxCriteria))
MODEL_OBJECTS.append(ModelClassDescription(ModelThresholdsMetric))
MODEL_OBJECTS.append(ModelClassDescription(ModelTopnMetricList))
MODEL_OBJECTS.append(ModelClassDescription(ModelScoreGroupThreshold))

atom_learn_query_sql = "\n".join([
    "SELECT",
    "	a.*",
    "FROM",
    "	db_model_atomlearn a",
    "INNER JOIN db_model_atomact b ON a.project_id = b.project_id",
    "AND a.component_id = b.atom_learn_id",
    "WHERE",
    "	a.project_id = '{project_id}'",
    "AND b.component_id = '{component_id}'"
])


@auto_param
def save(request, project_id, component_id, atom_act_id, input_comp_id):
    atom_learn = list(AtomLearn.objects.raw(
        atom_learn_query_sql.format(project_id=project_id,component_id=atom_act_id)
    ))
    if len(atom_learn) == 0:
        return HttpResponse(Response.fail(ERRORS.ATOM_ACT_NOT_CONFIGURED, None).to_json())
    atom_learn = atom_learn[0]
    assert isinstance(atom_learn, AtomLearn)
    learn_input_type = extract_component_type(atom_learn.input_comp_id)
    test_input_type = extract_component_type(input_comp_id)
    feature_id = atom_learn.feature_id
    feature_target = atom_learn.feature_target

    if test_input_type == COMPONENTS.HIVE_READER:
        fields = IOFieldType.objects.filter(project_id=project_id, component_id=input_comp_id,
                                            field__in=[feature_id, feature_target])
    elif test_input_type == COMPONENTS.ROBOTX_SPARK:
        fields = list(IOFieldType.objects.raw(robotx_field_in_query.format(
            project_id=project_id,
            component_id=input_comp_id,
            id=feature_id,
            target=feature_target
        )))
    elif test_input_type == COMPONENTS.FEATURE_COMBINE:
        fields = list(IOFieldType.objects.raw(combine_field_in_query.format(
            project_id=project_id,
            component_id=input_comp_id,
            id=feature_id,
            target=feature_target
        )))
    if len(fields) != 2:
        return HttpResponse(Response.fail(ERRORS.INPUT_NOT_SAME_AS_LEARN, None).to_json())
    AtomTest.objects.filter(project_id=project_id, component_id=component_id).delete()
    AtomTest(project_id=project_id, component_id=component_id, atom_act_id=atom_act_id,
            input_comp_id=input_comp_id).save()
    if learn_input_type != test_input_type:
        return HttpResponse(Response.success(ERRORS.COMPONENT_NOT_SAME_AS_LEARN).to_json())
    return HttpResponse(Response.success().to_json())


@auto_param
def report(request, project_id, component_id):
    res = dict()
    for model_obj in MODEL_OBJECTS:
        prop = model_obj.name
        values = model_obj.cls.objects.filter(project_id=project_id, component_id=component_id)
        if len(values) == 0:
            if model_obj == ModelSyntheticMetrics:
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
        content = Report.test_report(project_id, component_id, doc.width, threshold_confusion, threshold_top)
        doc.build(content)
        response.write(temp.getvalue())
    return response

