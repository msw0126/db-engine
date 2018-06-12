package com.taodatarobot.engine.py4j.hive.bean;

import java.util.List;


public class UploadToHiveConfig {
    private String gen_table;
    private String csv_file;

    public String getGen_table() {
        return gen_table;
    }

    public void setGen_table(String gen_table) {
        this.gen_table = gen_table;
    }

    public String getCsv_file() {
        return csv_file;
    }

    public void setCsv_file(String csv_file) {
        this.csv_file = csv_file;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getUpload_hdfs_path() {
        return upload_hdfs_path;
    }

    public void setUpload_hdfs_path(String upload_hdfs_path) {
        this.upload_hdfs_path = upload_hdfs_path;
    }

    public List<FieldType> getField_type() {
        return field_type;
    }

    public void setField_type(List<FieldType> field_type) {
        this.field_type = field_type;
    }

    private String delimiter;
    private String upload_hdfs_path;
    private List<FieldType> field_type;



}
