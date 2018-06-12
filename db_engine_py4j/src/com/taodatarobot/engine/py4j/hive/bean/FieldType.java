package com.taodatarobot.engine.py4j.hive.bean;

import java.util.ArrayList;

public class FieldType{
    private String name;
    private String type;
    private ArrayList<String> sampleData;

    public FieldType(){}

    public FieldType(String name, String type){
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ArrayList<String> getSampleData() {
        return sampleData;
    }

    public void setSampleData(ArrayList<String> sampleData) {
        this.sampleData = sampleData;
    }

    public void setSample(String sample){
        if(this.sampleData==null){
            this.sampleData = new ArrayList<>();
        }
        this.sampleData.add(sample);
    }
}