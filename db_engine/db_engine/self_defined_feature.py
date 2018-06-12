import csv
from typing import List

import os
from django.http import HttpResponse

from common import ERRORS
from common.UTIL import auto_param, mk_working_directory, Response, is_date
from db_model.models import SelfDefinedFeatureType, SelfDefinedFeature


class FieldType:

    def __init__(self, field, field_type=None,ori_type=None,date_format=None,date_size=None, sample_data=None, selected=True):
        self.field = field
        self.field_type = field_type
        self.ori_type = ori_type
        self.sample_data = [] if sample_data is None else sample_data
        self.date_format = date_format
        self.date_size = date_size
        if isinstance(selected,bool):
            self.selected = selected
        else:
            self.selected = (selected=='true')

    def guess_field_type(self):
        not_null_num = 0
        numeric_num = 0
        fix_length = -1
        dot_in_it = False
        for sample in self.sample_data:
            if sample is None:
                continue
            not_null_num += 1
            if fix_length == -1:
                fix_length = len(sample)
            elif fix_length != -2:
                if fix_length != len(sample):
                    fix_length = -2
            if "." in sample and not dot_in_it:
                dot_in_it = True
            try:
                float(sample)
                numeric_num += 1
            except Exception as e:
                pass
        if not_null_num == 0:
            self.field_type = 'factor'
        elif not_null_num == numeric_num:
            self.field_type = 'numeric'
            if not dot_in_it and fix_length > 0:
                if fix_length == 18 or fix_length == 11:
                    self.field_type = 'factor'
        else:
            date_, size_ = is_date(self.sample_data)
            # print(self.sample_data)
            if date_:
                self.field_type = 'date'
                self.date_format = size_
                self.date_size = size_
            else:
                self.field_type = 'factor'
        self.ori_type = self.field_type

    def add_sample_data(self, sample):
        if sample == '':
            sample = None
        self.sample_data.append(sample)

    def to_db_type(self, project_id, component_id):
        if isinstance(self.sample_data, list):
            sample_data_trim = []
            for idx, sample in enumerate(self.sample_data):
                if idx>2: break
                if sample is None:
                    sample_data_trim.append("")
                else:
                    sample_data_trim.append(sample[:300])
            self.sample_data = '","'.join(sample_data_trim)
        return SelfDefinedFeatureType(project_id= project_id,
                               component_id= component_id,
                               field = self.field,
                               field_type= self.field_type,
                               ori_type= self.ori_type,
                               date_format = self.date_format,
                               date_size = self.date_size,
                               selected = self.selected,
                               sample_data= self.sample_data)



@auto_param
def upload(request, project_id, component_id, file):
    # 保存文件
    file_name = file.name
    data_saving_path = mk_working_directory(project_id, component_id, 'data.csv')
    with open(data_saving_path, 'wb') as destination:
        if file.multiple_chunks():
            for chunk in file.chunks():
                destination.write(chunk)
        else:
            destination.write(file.read())
    # 检查文件，判断数据类型
    response = None
    field_types = None  # type: dict[str,FieldType]
    try:
        header = None
        column_num = -1
        with(open(data_saving_path, 'r', encoding='utf-8')) as f:
            csv_reader = csv.reader(f)
            for row_num, row in enumerate(csv_reader):
                if row_num>21:
                    break
                if header is None:
                    column_num = len(row)
                    if column_num < 2:
                        # csv列数量太少
                        response = Response.fail(ERRORS.CSV_COLUMN_SIZE_ERROR,None)
                        return HttpResponse(response.to_json())
                    header = row
                    field_types = {column : FieldType(column) for column in row}
                else:
                    len_of_column = len(row)
                    if len_of_column!=column_num:
                        response = Response.fail(ERRORS.CSV_COLUMN_NUM_ERROR, dict(
                            header_column_num=column_num,
                            line=row_num+1,
                            row_column_num = len_of_column
                        ))
                        return HttpResponse(response.to_json())
                    for column, sample in zip(header, row):
                        field_types[column].add_sample_data(sample)
        if header is None:
            response = Response.fail(ERRORS.CSV_EMPTY, None)
            return HttpResponse(response.to_json())
        if len(field_types[header[0]].sample_data)<20:
            response = Response.fail(ERRORS.CSV_ROW_TOO_SMALL, None)
            return HttpResponse(response.to_json())
        # 数据类型判断
        db_field_types = []
        # fields = field_types.values()
        # sorted(fields, key=lambda x:x.field)
        for field in field_types.values():
            field.guess_field_type()
            db_field_types.append(field.to_db_type(project_id, component_id))
        # 保存组件
        SelfDefinedFeature.objects.filter(project_id=project_id,component_id=component_id).delete()
        SelfDefinedFeature(project_id=project_id,component_id=component_id,file_name=file_name).save()

        # 保存类型
        SelfDefinedFeatureType.objects.filter(project_id=project_id, component_id=component_id).delete()
        SelfDefinedFeatureType.objects.bulk_create(db_field_types)

        response = Response.success(list(field_types.values()))
        return HttpResponse(response.to_json())
    except UnicodeDecodeError as e:
        response = Response.fail(ERRORS.CSV_UTF8_ERROR, None)
        return HttpResponse(response.to_json())


@auto_param
def save_field_type(request, project_id, component_id, field_types: List[FieldType]):
    db_field_types = []
    for field in field_types:
        db_field_types.append(field.to_db_type(project_id, component_id))
    # 保存类型
    SelfDefinedFeatureType.objects.filter(project_id=project_id, component_id=component_id).delete()
    SelfDefinedFeatureType.objects.bulk_create(db_field_types)
    response = Response.success()
    return HttpResponse(response.to_json())


@auto_param
def load_field_type(request, project_id, component_id):
    objs = SelfDefinedFeature.objects.filter(project_id=project_id, component_id=component_id)
    if len(objs)!=1:
        response = Response.fail(ERRORS.COMPONENT_NOT_EXIST, None)
        return HttpResponse(response.to_json())
    db_field_types = SelfDefinedFeatureType.objects.filter(project_id=project_id, component_id=component_id)
    field_types = list()
    for db_field_type in db_field_types:
        field_types.append(FieldType(db_field_type.field,
                                     db_field_type.field_type,
                                     db_field_type.ori_type,
                                     db_field_type.date_format,
                                     db_field_type.date_size,
                                     db_field_type.sample_data,
                                     db_field_type.selected))
    response = Response.success(field_types)
    return HttpResponse(response.to_json())


@auto_param
def perview(request, project_id, component_id):
    # self_defined_feature = SelfDefinedFeature.objects.filter(project_id=project_id, component_id=component_id)
    # if len(self_defined_feature)==0:
    #     return HttpResponse(Response.fail(ERRORS.NOT_INITED, None).to_json())
    data_saving_path = mk_working_directory(project_id, component_id, 'data.csv')
    if not os.path.exists( data_saving_path ):
        return HttpResponse( Response.fail( ERRORS.CSV_NOT_UPLOAD, None ).to_json() )
    result = list()
    with(open(data_saving_path, 'r', encoding='utf-8')) as f:
        csv_reader = csv.reader(f)
        for row_num, row in enumerate(csv_reader):
            if row_num > 10:
                break
            if len(result) == 0:
                for col in row:
                    result.append(dict(
                        name = col,
                        value = list()
                    ))
            else:
                for column, sample in zip(result, row):
                    column['value'].append(sample)
    return HttpResponse(Response.success(result).to_json())
