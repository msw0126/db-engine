from common.UTIL import auto_param, Response, py4j_common_hive_util, FIELDTYPE, is_date
from common import ERRORS, VALIDATE
from django.http import HttpResponse

from db_model.models import HiveReader

DATABASE_MAPPING = dict(
    TINYINT = FIELDTYPE.NUMERIC,
    SMALLINT = FIELDTYPE.NUMERIC,
    INT = FIELDTYPE.NUMERIC,
    INTEGER = FIELDTYPE.NUMERIC,
    BIGINT = FIELDTYPE.NUMERIC,
    FLOAT = FIELDTYPE.NUMERIC,
    DOUBLE = FIELDTYPE.NUMERIC,
    DECIMAL = FIELDTYPE.NUMERIC,
    NUMERIC = FIELDTYPE.NUMERIC,
    TIMESTAMP = FIELDTYPE.DATE,
    DATE = FIELDTYPE.DATE,
    STRING = FIELDTYPE.FACTOR,
    VARCHAR = FIELDTYPE.FACTOR,
    CHAR = FIELDTYPE.FACTOR
)


class StructureClass:

    def __init__(self, field, field_type, database_type, date_format=None, date_size=None, ignore=False, selected=True):
        self.field = field
        self.database_type = database_type
        self.field_type = field_type
        self.date_format = date_format
        self.date_size = date_size
        if isinstance(ignore, bool):
            self.ignore = ignore
        else:
            self.ignore = ignore == 'true'
        if self.ignore:
            self.selected = False
        elif isinstance(selected, bool):
            self.selected = selected
        else:
            self.selected = selected == 'true'


@auto_param
def list_table(request, query_str: str=None):
    param = list()
    if query_str is None or query_str.strip()=='':
        # 参数为空，调用listTable，获取所有的表
        func = 'listTable'
    else:
        # 参数非空，模糊匹配表
        func = 'queryTable'
        param.append(query_str.strip())

    result = py4j_common_hive_util(func, *param)
    if isinstance(result, HttpResponse):
        return result
    response = Response.success(list(result))
    return HttpResponse(response.to_json())


@auto_param
def structure(request, table_name):
    # 检查 table_name 为非空
    check = VALIDATE.not_null_validate(table_name, 'table_name')
    if check is not None:
        response = Response.fail(ERRORS.PARAMETER_VALUE_ERROR, check)
        return HttpResponse(response.to_json())

    result = py4j_common_hive_util('checkExist', table_name)
    if isinstance(result, HttpResponse):
        return result
    if not result:
        return HttpResponse(Response.fail(ERRORS.HIVE_TABLE_NOT_EXIST, None).to_json())

    result = py4j_common_hive_util('describeAndSample', table_name)
    result = list(result)
    result_trans = list()
    # 从数据库类型映射到建模类型
    # 1. 不支持的数据类型，标记 ignore为 true
    # 2. 日期类型，标记前端，可选的最小粒度，记录在字段 date_format中
    for field_desc in result:
        field = field_desc.getName()
        database_type_trans = field_desc.getType()
        ignore = True
        field_type = None
        date_format = None
        date_size = None
        if database_type_trans in DATABASE_MAPPING:
            ignore = False
            field_type = DATABASE_MAPPING[database_type_trans]

            sample_data = field_desc.getSampleData()
            if field_type == FIELDTYPE.FACTOR:
                if sample_data is not None:
                    sample_data = list(sample_data)
                    date_, size_ = is_date(sample_data)
                    if date_:
                        date_format = size_
                        date_size = size_
                        field_type = FIELDTYPE.DATE

            if database_type_trans == 'TIMESTAMP':
                date_format = 'second'
                date_size = 'second'
            elif database_type_trans == 'DATE':
                date_format = 'day'
                date_size = 'day'
        struct = StructureClass(field, field_type, database_type_trans, date_format, date_size, ignore)
        result_trans.append(struct)
    # result_trans.sort(key=lambda x: x.field)
    response = Response.success(result_trans)
    return HttpResponse(response.to_json())


@auto_param
def preview(result, project_id, component_id):
    reader = HiveReader.objects.filter(project_id=project_id, component_id=component_id)
    if len(reader)==0:
        return HttpResponse(Response.fail(ERRORS.HIVE_TABLE_NOT_EXIST).to_json())
    reader = reader[0]
    table_name = reader.table_name
    result = py4j_common_hive_util('viewTable', table_name, 10)
    if isinstance(result, HttpResponse):
        return result
    return HttpResponse(Response.success([dict(name=k, value=list(v)) for k, v in result.items()]).to_json())

