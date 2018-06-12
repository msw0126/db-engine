import csv
import os
from typing import List

import time

import math
from django.http import HttpResponse
from py4j.java_gateway import JavaGateway

from F_SETTING import WORKING_DIRECTORY, CLUSTER_DIRECTORY
from common import ERRORS
from common.UTIL import auto_param,  Response, is_date, py4j_common_hive_util
from db_model.models import MyData, MyDataType, MyDataCsvInfo


class FileNames:

    def __init__(self, filename):

        self.filename = filename


class FieldType:

    def __init__(self, field,field_type=None,ori_type=None,date_format=None,date_size=None, sample_data=None, selected=True):
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

    def to_db_type(self, hive_filename):
        if isinstance(self.sample_data, list):
            sample_data_trim = []
            for idx, sample in enumerate(self.sample_data):
                if idx>2: break
                if sample is None:
                    sample_data_trim.append("")
                else:
                    sample_data_trim.append(sample[:300])
            self.sample_data = '","'.join(sample_data_trim)
        return MyDataType(file_name=hive_filename,
                                field=self.field,
                               field_type= self.field_type,
                               ori_type= self.ori_type,
                               date_format = self.date_format,
                               date_size = self.date_size,
                               selected = self.selected,
                               sample_data= self.sample_data)


def load_into_hive(table_name, csv_path, hdfs_path, hdfs_path_dir,  fild):
    gateway = None
    try:
        gateway = JavaGateway()
        ff = gateway.jvm.java.util.ArrayList()
        for k in fild:
            ff.append(k)
        hive_util = gateway.entry_point.getHiveUtil()
        func = hive_util.uploadToHive
        func(table_name, csv_path, hdfs_path, hdfs_path_dir, ff)
    except Exception as e:
        raise e
    finally:
        if gateway is not None:
            gateway.close()


def format_size(byte):
    try:
        bytes = float( byte )
        kb = bytes / 1024
    except Exception as e:
        print( "传入的字节格式不对" )
        raise e

    if kb >= 1024:
        M = kb / 1024
        if M >= 1024:
            G = M / 1024
            return "%sG" % (round(G, 2))
        else:
            return "%sM" % (round(M, 2))
    else:
        return "%skb" % (round(kb, 2))


def get_file_size(path):
    try:
        size = os.path.getsize( path )
        return format_size( size )
    except Exception as err:
        raise err


def time_stamp_time(timestamp):
    time_struct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_struct)


def get_file_create_time(file_path):
    t = os.path.getctime(file_path)
    return time_stamp_time(t)


def delete_csv_first_row(origin_f , new_f):
    """删除上传的csv文件的第一行"""

    with open(origin_f, encoding="utf-8") as fp_in:
        with open( new_f, 'w', encoding="utf-8" ) as fp_out:
            fp_out.writelines( line for i, line in enumerate( fp_in ) if i != 0)


def check_hive_mysql(mysql_table_sum):
    """查询hive仓库的表，如果没有在mysql中，则认为是手工直接在hive创建的，并保存表名到mysql"""

    search_from_hive_table_list = py4j_common_hive_util('listTable')
    if isinstance(search_from_hive_table_list, HttpResponse):
        return search_from_hive_table_list
    search_from_mysql_table_list = []
    for row_obj in mysql_table_sum:
        search_from_mysql_table_list.append(row_obj.file_name)
        if row_obj.file_name not in search_from_hive_table_list and len(search_from_hive_table_list) > 0:
            MyData.objects.filter(file_name=row_obj.file_name).delete()
    for hive_table in search_from_hive_table_list:
        if hive_table not in search_from_mysql_table_list:
            MyData(file_name=hive_table).save()



@auto_param
def csv_upload(request, file, filename):
    # 保存文件
    hive_filename = filename.strip()
    csv_file_name = file.name
    mydata_directory = os.path.join(WORKING_DIRECTORY, "mydata")
    if not os.path.exists(mydata_directory):
        os.makedirs(mydata_directory)
    csv_saving_path = os.path.join(mydata_directory, "%s.csv" % hive_filename)
    with open(csv_saving_path, 'wb') as destination:
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
        with(open(csv_saving_path, 'r', encoding='utf-8')) as f:
            csv_reader = csv.reader(f)
            for row_num, row in enumerate(csv_reader):
                #循环21行结束
                if row_num>21:
                    break
                if header is None:
                    #循环到第一行
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
                        #如果第二行与第一行的列数不相等，返回错误"列数不一致"
                        response = Response.fail(ERRORS.CSV_COLUMN_NUM_ERROR, dict(
                            header_column_num=column_num,
                            line=row_num+1,
                            row_column_num = len_of_column
                        ))
                        return HttpResponse(response.to_json())
                    for column, sample in zip(header, row):
                        #得到sampledata的数据，循环21次会有20行的数据
                        field_types[column].add_sample_data(sample)
        if header is None:
            response = Response.fail(ERRORS.CSV_EMPTY, None)
            return HttpResponse(response.to_json())
        if len(field_types[header[0]].sample_data)<20:
            response = Response.fail(ERRORS.CSV_ROW_TOO_SMALL, None)
            return HttpResponse(response.to_json())
        #判断文件名称是否相同
        objs = MyData.objects.filter(file_name=hive_filename)
        if len(objs) != 0:
            response = Response.fail(ERRORS.CSV_NAME_REPEAT, None)
            return HttpResponse(response.to_json())
        # 数据类型判断
        db_field_types = []
        for field in field_types.values():
            field.guess_field_type()
            db_field_types.append(field.to_db_type(hive_filename))
        # 保存文件名称
        MyDataCsvInfo.objects.filter(file_name=hive_filename).delete()
        MyDataCsvInfo(file_name=hive_filename,csv_file_name=csv_file_name).save()

        # 保存类型
        MyDataType.objects.filter(file_name=hive_filename).delete()
        MyDataType.objects.bulk_create(db_field_types)

        response = Response.success(list(field_types.values()))
        return HttpResponse(response.to_json())
    except UnicodeDecodeError as e:
        response = Response.fail(ERRORS.CSV_UTF8_ERROR, None)
        return HttpResponse(response.to_json())


@auto_param
def perview(request, filename):
    hive_filename = filename.strip()
    csv_saving_path = os.path.join(WORKING_DIRECTORY, "mydata", hive_filename)
    result = list()
    with(open(csv_saving_path, 'r', encoding='utf-8')) as f:
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


@auto_param
def csv_into_hive(request, filename, username, field_types: List[FieldType]):
    #读取csv文件，获得字段名
    csv_field_list = None
    field_num = None
    hive_filename = filename.strip()
    csv_saving_path = os.path.join(WORKING_DIRECTORY, "mydata", "%s.csv" % hive_filename)
    with(open(csv_saving_path, 'r', encoding='utf-8')) as f:
        load_hive_csv_reader = csv.reader(f)
        for row_num, row in enumerate(load_hive_csv_reader):
            csv_field_list = row
            field_num = len(row)
            break
    #上传时保存数据信息
    file_size = get_file_size( csv_saving_path )
    creat_time = get_file_create_time( csv_saving_path )
    MyData( file_name=hive_filename, field_num=field_num, file_size=file_size,
            creat_time=creat_time, creat_user=username ).save()

    # 手动选择保存字段类型
    db_field_types = []
    for field in field_types:
        db_field_types.append( field.to_db_type( hive_filename) )
    # 保存类型
    MyDataType.objects.filter( file_name=hive_filename ).delete()
    MyDataType.objects.bulk_create( db_field_types )

    field_list = []
    for field in csv_field_list:
        objcs = MyDataType.objects.filter(file_name=hive_filename, field=field)
        for objcs_field_type in objcs:
            field_type = objcs_field_type.field_type
            sample_field_type = objcs_field_type.sample_data
            if field_type == "numeric":
                field_type = "bigint"
            elif field_type == "factor":
                field_type = "string"
            elif field_type == "date":
                field_type = "date"
            else:
                field_type = "string"
            #如果数据包含小数点，则认为是Double类型
            for x in sample_field_type:
                if "." in x:
                    field_type = "Double"
            field_and_type = format("`%s` %s" %(field, field_type))
            field_list.append(field_and_type)
    hdfs_path = os.path.join(CLUSTER_DIRECTORY, "mydata", "%s", "%s.csv") % (hive_filename, hive_filename)
    hdfs_path_dir = os.path.join(CLUSTER_DIRECTORY, "mydata", "%s" % hive_filename)
    # print(hdfs_path)
    # print(hdfs_path_dir)
    new_csv_saving_path = os.path.join(WORKING_DIRECTORY, "mydata", "%s_.csv" % hive_filename)
    try:
        delete_csv_first_row(csv_saving_path , new_csv_saving_path)
        load_into_hive(hive_filename, new_csv_saving_path, hdfs_path, hdfs_path_dir, field_list)
    except Exception as e:
        MyData.objects.filter(file_name=hive_filename).delete()
        # 删除hive表
        result = py4j_common_hive_util( "dropTable", hive_filename )
        if isinstance( result, HttpResponse ):
            return result
        # 删除hdfs上保存的本地csv文件
        delete_hdfs = py4j_common_hive_util( "deleteHdfsFile", hdfs_path_dir )
        if isinstance( delete_hdfs, HttpResponse ):
            return delete_hdfs
        # 删除服务器上的csv文件
        mydata_directory = os.path.join( WORKING_DIRECTORY, "mydata" )
        csv_saving_path = os.path.join( mydata_directory, "%s.csv" % hive_filename )
        new_csv_saving_path = os.path.join( mydata_directory, "%s_.csv" % hive_filename )
        if os.path.exists( csv_saving_path ):
            os.remove( csv_saving_path )
        if os.path.exists( new_csv_saving_path ):
            os.remove( new_csv_saving_path )
        #raise e
        response = Response.fail(ERRORS.CSV_INTO_ERROR, None)
        return HttpResponse(response.to_json())
    return HttpResponse(Response.success().to_json())


@auto_param
def list_table(request, index, page_num):
    object = MyData.objects.all().order_by('creat_time').reverse()[(int(index) - 1) * int(page_num):int(index) * int(page_num)]
    sum_data = MyData.objects.all()
    #检查hive里的表是否全部在mysql中已存储信息
    check_hive_mysql(sum_data)

    sum_index = len(sum_data) / int(page_num)
    remainder = len( sum_data ) % int(page_num)
    if remainder > 0:
        sum_index_page = int(sum_index + 1)
    else:
        sum_index_page = math.floor(sum_index)
    sum_index_x = dict()
    sum_index_x['sum_index'] = sum_index_page
    json_data = []
    for row_obj in object:
        result = dict()  # temp store one jsonObject
        result['file_name'] = row_obj.file_name
        result['field_num'] = row_obj.field_num
        result['file_size'] = row_obj.file_size
        result['craet_time'] = row_obj.creat_time
        result['creat_user'] = row_obj.creat_user
        json_data.append( result )
    json_data.append(sum_index_x)
    return HttpResponse( Response.success(json_data).to_json())


@auto_param
def delete_table(request, filenames: List[FileNames]):
    for table in filenames:
        table = table.filename
        hdfs_path_dir = os.path.join(CLUSTER_DIRECTORY, "mydata", "%s" % table)
        # 删除hive表
        result = py4j_common_hive_util( "dropTable", table )
        if isinstance( result, HttpResponse ):
            return result
        #删除hdfs上保存的本地csv文件
        delete_hdfs = py4j_common_hive_util("deleteHdfsFile", hdfs_path_dir)
        if isinstance(delete_hdfs, HttpResponse):
            return delete_hdfs
        #删除服务器上的csv文件
        mydata_directory = os.path.join( WORKING_DIRECTORY, "mydata" )
        csv_saving_path = os.path.join( mydata_directory, "%s.csv" % table )
        new_csv_saving_path = os.path.join( mydata_directory, "%s_.csv" % table )
        if os.path.exists( csv_saving_path ):
            os.remove( csv_saving_path )
        if os.path.exists( new_csv_saving_path ):
            os.remove( new_csv_saving_path )
        #删除mysql数据库
        MyData.objects.filter( file_name=table ).delete()
        MyDataCsvInfo.objects.filter( file_name=table ).delete()
    return HttpResponse(Response.success().to_json())


@auto_param
def search_table(request, filename, index, page_num):
    object = MyData.objects.filter(file_name__icontains= filename).order_by('creat_time').reverse() \
        [(int(index) - 1) * int(page_num):int(index) * int(page_num)]
    sum_data = MyData.objects.filter(file_name__icontains= filename)
    print(len(sum_data))
    sum_index = len(sum_data) / int(page_num)
    remainder = len(sum_data) % int(page_num)
    if remainder > 0:
        sum_index_page = int( sum_index + 1 )
        print( sum_index_page )
    else:
        sum_index_page = math.floor( sum_index )
        print( sum_index_page )
    sum_index_x = dict()
    sum_index_x['sum_index'] = sum_index_page
    if len(object) == 0:
        return HttpResponse(Response.success().to_json())
    json_data = []
    for row_obj in object:
        result = dict()  # temp store one jsonObject
        result['file_name'] = row_obj.file_name
        result['field_num'] = row_obj.field_num
        result['file_size'] = row_obj.file_size
        result['craet_time'] = row_obj.creat_time
        result['creat_user'] = row_obj.creat_user
        json_data.append( result )
    json_data.append( sum_index_x )
    return HttpResponse( Response.success( json_data ).to_json() )
