from django.http import HttpResponse, StreamingHttpResponse
from io import BytesIO

from reportlab.platypus import SimpleDocTemplate

from common import ERRORS
from common.UTIL import auto_param, Response, COMPONENTS,extract_component_type
from common.report import Report
from db_model.models import AtomLearn, IOFieldType, AtomAct, ModelPredictionBIns
from db_engine.atom_learn import robotx_field_in_query, combine_field_in_query, ModelClassDescription
from executor.components.AtomAct import AtomAct as AtomActExecutor

MODEL_OBJECTS = list()
MODEL_OBJECTS.append(ModelClassDescription(ModelPredictionBIns))


@auto_param
def save(request, project_id, component_id, atom_learn_id, input_comp_id):
    atom_learn = AtomLearn.objects.filter(project_id=project_id, component_id=atom_learn_id)
    if len(atom_learn) == 0:
        return HttpResponse(Response.fail(ERRORS.ATOM_LEARN_NOT_CONFIGURED, None).to_json())
    atom_learn = atom_learn[0]
    assert isinstance(atom_learn, AtomLearn)
    learn_input_type = extract_component_type(atom_learn.input_comp_id)
    act_input_type = extract_component_type(input_comp_id)
    feature_id = atom_learn.feature_id

    if act_input_type == COMPONENTS.HIVE_READER:
        fields = IOFieldType.objects.filter(project_id=project_id, component_id=input_comp_id,
                                            field__in=[feature_id])
    elif act_input_type == COMPONENTS.ROBOTX_SPARK:
        fields = list(IOFieldType.objects.raw(robotx_field_in_query.format(
            project_id=project_id,
            component_id=input_comp_id,
            id=feature_id,
            target=''
        )))
    elif act_input_type == COMPONENTS.FEATURE_COMBINE:
        fields = list(IOFieldType.objects.raw(combine_field_in_query.format(
            project_id=project_id,
            component_id=input_comp_id,
            id=feature_id,
            target=''
        )))
    if len(fields)!=1:
        return HttpResponse(Response.fail(ERRORS.INPUT_NOT_SAME_AS_LEARN, None).to_json())
    AtomAct.objects.filter(project_id=project_id,component_id=component_id).delete()
    AtomAct(project_id=project_id,component_id=component_id,atom_learn_id=atom_learn_id,input_comp_id=input_comp_id).save()
    if learn_input_type != act_input_type:
        return HttpResponse(Response.success(ERRORS.COMPONENT_NOT_SAME_AS_LEARN).to_json())
    return HttpResponse(Response.success().to_json())


def file_iterator(file_name, threshold):
    first_line = True
    with open(file_name,'r') as f:
        while True:
            c = f.readline()
            if c:
                if first_line:
                    first_line = False
                    yield c
                else:
                    id, predict, p0, p1 = tuple(c.split(","))
                    if float(p1.strip()) >= threshold:
                        predict = "1"
                    else:
                        predict = "0"
                    yield ",".join([id, predict, p0, p1])
            else:
                break


@auto_param
def download_prediction(request, project_id, component_id, threshold=0.5):
    prediction_path = AtomActExecutor.get_prediction_csv_local_path(project_id, component_id)
    threshold = float(threshold)
    response = StreamingHttpResponse(file_iterator(prediction_path, threshold))
    response['Content-Type'] = 'application/octet-stream'
    file_name = "%s_%s_%s" %(project_id, component_id, AtomActExecutor.PREDICTION_CSV)
    response['Content-Disposition'] = 'attachment;filename="{0}"'.format(file_name)
    return response


@auto_param
def report(request, project_id, component_id):
    res = dict()
    for model_obj in MODEL_OBJECTS:
        prop = model_obj.name
        values = model_obj.cls.objects.filter(project_id=project_id, component_id=component_id)
        if len(values) == 0:
            if model_obj == ModelPredictionBIns:
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
def report_pdf(request, project_id, component_id, threshold):
    threshold = float(threshold)

    response = HttpResponse(content_type='application/pdf')
    response['Content-Disposition'] = 'attachment; filename=%s_%s.pdf' %( project_id, component_id)

    with BytesIO() as temp:
        doc = SimpleDocTemplate(temp)
        content = Report.act_report(project_id, component_id, doc.width, threshold)
        doc.build(content)
        response.write(temp.getvalue())
    return response
