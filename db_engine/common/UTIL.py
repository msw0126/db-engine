import re

import os
import shutil
from datetime import datetime

from django.http import HttpResponse
import inspect
import json

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JNetworkError, Py4JJavaError

from common import ERRORS
import typing

# 参数自动拼装
from setting import WORKING_DIRECTORY


def auto_param(func):
    def wrapper(*arg, **kwargs):
        request = arg[0]
        if(request.method=='GET'):
            param_getter = request.GET
        else:
            # POST请求中，如果有文件，需要把文件也放入字典
            param_getter = request.POST
            files = request.FILES
            if len(files)>0:
                param_getter = dict()
                for field_name, value in request.POST.items():
                    param_getter[field_name] = value
                for field_name, file in files.items():
                    param_getter[field_name] = file
        params = inspect.signature(func).parameters

        missing_params = []
        if(len(params)!=1):
            for idx, param in enumerate(params):
                if idx == 0: continue
                describe = params.get(param)
                parameter_setting(param, describe, param_getter, missing_params, kwargs)
        if(len(missing_params)>0):
            resp = Response.fail(ERRORS.PARAMETER_MISSING_ERROR, "missing param: %s" %",".join(missing_params))
            return HttpResponse(resp.to_json())
        return func(*arg, **kwargs)
    return wrapper

def parameter_setting(param, describe, param_getter, missing_params, kwargs):
    annotation = describe.annotation
    if param in param_getter:
        kwargs[param] = param_getter[param]
    elif isinstance(annotation,typing.GenericMeta) and "typing.List" in str(annotation):
        subclass = annotation.__parameters__[0]
        obj_map = dict()
        if subclass == str or subclass == int:
            param_list_key = "%s[]" %param
            if param_list_key in param_getter:
                kwargs[param] = param_getter.getlist(param_list_key)
            elif describe.default == inspect._empty:
                missing_params.append(param)
        elif hasattr(subclass, '__init__'):
            reg = re.compile('%s\[(\d+)\]\[([\w_][\w\d_]*)\]' % param)
            for k, v in param_getter.items():
                finds = re.findall(reg, k)
                if len(finds)!=1 or len(finds[0])!=2:
                    continue
                idx = int(finds[0][0])
                param_name = finds[0][1]
                if idx not in obj_map:
                    obj_map[idx] = dict()
                obj_map[idx][param_name] = v
            kwargs[param] = [subclass(**obj_map[idx]) for idx in sorted(obj_map.keys())]
        else:
            missing_params.append(param)
    elif describe.default == inspect._empty:
        # 如果参数没有默认值，标记为传参错误
        missing_params.append(param)


class Response(object):
    """
    通用返回类型
    成功，返回 数据
    失败，返回 错误码，错误细节
    """

    def __init__(self, error_code, detail):
        if error_code is not None:
            self.error_code = error_code
        self.detail = detail

    @staticmethod
    def success(detail=None):
        return Response(None, detail)

    @staticmethod
    def fail(error_code, detail=None):
        return Response(error_code, detail)

    def to_json(self):
        return to_json(self)


def to_json(obj, indent=0):
    """
    json格式化
    """
    obj_t = __trans_to_ser__(obj)
    return json.dumps(obj_t, indent=indent)


def __trans_to_ser__(obj):
    if hasattr(obj, '__dict__'):
        return __trans_to_ser__(obj.__dict__)
    elif isinstance(obj, list):
        obj_n = list()
        for v in obj:
            obj_n.append(__trans_to_ser__(v))
        return obj_n
    elif isinstance(obj, dict):
        obj_n = dict()
        for k,v in obj.items():
            obj_n[k] = __trans_to_ser__(v)
        return obj_n
    elif isinstance(obj, set):
        obj_n = list()
        for v in obj:
            obj_n.append(__trans_to_ser__(v))
        return obj_n
    elif isinstance(obj, datetime):
        return str(obj)
    else:
        return obj


def py4j_common_hive_util(func, *param):
    gateway = None
    try:
        gateway = JavaGateway()
        hive_util = gateway.entry_point.getHiveUtil()
        func = getattr(hive_util,func)
        result = func(*param)
    except Py4JNetworkError as e:
        response = Response.fail(ERRORS.PY4J_CONNECTION_ERROR,None)
        return HttpResponse(response.to_json())
    except Py4JJavaError as e:
        # 查询出错，返回错误信息
        response = Response.fail(ERRORS.HIVE_QUERY_ERROR, e.java_exception.getMessage())
        return HttpResponse(response.to_json())
    finally:
        if gateway is not None:
            gateway.close()
    return result


class FIELDTYPE:
    NUMERIC = 'numeric'
    FACTOR = 'factor'
    DATE = 'date'


class COMPONENTS:
    SELF_DEFINED_FEATURE = "SelfDefinedFeature"
    HIVE_READER = 'HiveReader'
    ROBOTX_SPARK = 'RobotXSpark'
    ATOM_LEARN = 'AtomLearn'
    ATOM_ACT = "AtomAct"
    ATOM_TEST = "AtomTest"
    FEATURE_COMBINE = "FeatureCombine"
    EXPORT_MODEL = "ExportModel"


def mk_working_directory(project_id, component_id, *external_path):
    component_directory = os.path.join(WORKING_DIRECTORY, project_id, component_id)
    if not os.path.exists(component_directory):
        os.makedirs(component_directory)
    return os.path.join(component_directory, *external_path)


def del_working_directory(project_id, component_id):
    component_directory = os.path.join(WORKING_DIRECTORY, project_id, component_id)
    if os.path.exists(component_directory):
        shutil.rmtree(component_directory)


def extract_component_type(component_id):
    """
    提取组件类型
    :param component_id:
    :return:
    """
    return re.sub('\d+', '', component_id)


DATE_MAPPING = {
    7 : (r"^\d\d{3}-[0-1]\d$", "month"),
    10: (r"^\d\d{3}-[0-1]\d-[0-3]\d$", "day"),
    13: (r"^\d\d{3}-[0-1]\d-[0-3]\d [0-2]\d$", "hour"),
    16: (r"^\d\d{3}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d$", "minute"),
    19: (r"^\d\d{3}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d$","second")
}


def is_date(sample_data):
    none_count = 0
    fmt = None
    rep = None
    for sample in sample_data:
        if sample is None:
            none_count += 1
            continue
        if len(sample) not in DATE_MAPPING:
            return False, None
        rep_, fmt_ = DATE_MAPPING[len(sample)]
        if rep is None:
            rep = rep_
            fmt = fmt_
        if fmt != fmt_:
            return False, None
        matched = re.match(rep_, sample, flags=0)
        if matched is None:
            return False, None
    if none_count == len(sample_data):
        return False, None
    return True, fmt


