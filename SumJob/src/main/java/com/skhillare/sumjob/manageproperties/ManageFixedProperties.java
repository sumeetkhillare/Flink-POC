package com.skhillare.sumjob.manageproperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ManageFixedProperties {
    //Manage fixedvalues.properties
    private String Value_Q_NAME;
    private String List_Value_Q_NAME;
    private String Map_Value_Q_NAME;
    private String Reduce_Value_Q_NAME;
    private String Keyed_Value_Q_NAME;
    private String statekey;
    private Properties properties;
    private String filename="fixedvalues.properties";
    public ManageFixedProperties(){
        properties=new Properties();
        try {
            InputStream in = ManageFixedProperties.class.getClassLoader().getResourceAsStream(filename);
            properties.load(in);
            this.properties=properties;
            this.statekey=properties.getProperty("statekey");
            this.Keyed_Value_Q_NAME=properties.getProperty("Keyed_Value_Q_NAME");
            this.List_Value_Q_NAME=properties.getProperty("List_Value_Q_NAME");
            this.Value_Q_NAME=properties.getProperty("Value_Q_NAME");
            this.Map_Value_Q_NAME=properties.getProperty("Map_Value_Q_NAME");
            this.Reduce_Value_Q_NAME=properties.getProperty("Reduce_Value_Q_NAME");
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public String getStatekey() {
        return statekey;
    }

    public String getValue_Q_NAME() {
        return Value_Q_NAME;
    }

    public String getList_Value_Q_NAME() {
        return List_Value_Q_NAME;
    }

    public String getMap_Value_Q_NAME() {
        return Map_Value_Q_NAME;
    }

    public String getKeyed_Value_Q_NAME() {
        return Keyed_Value_Q_NAME;
    }

    public String getReduce_Value_Q_NAME() { return Reduce_Value_Q_NAME; }

    public Properties getProperties() {
        return properties;
    }

    public String getFilename() {
        return filename;
    }

    public void setValue_Q_NAME(String value_Q_NAME) {
        Value_Q_NAME = value_Q_NAME;
        properties.setProperty("Value_Q_NAME",value_Q_NAME);
    }

    public void setList_Value_Q_NAME(String list_Value_Q_NAME) {
        List_Value_Q_NAME = list_Value_Q_NAME;
        properties.setProperty("List_Value_Q_NAME",list_Value_Q_NAME);
    }

    public void setMap_Value_Q_NAME(String map_Value_Q_NAME) {
        Map_Value_Q_NAME = map_Value_Q_NAME;
        properties.setProperty("Map_Value_Q_NAME",map_Value_Q_NAME);
    }

    public void setReduce_Value_Q_NAME(String reduce_Value_Q_NAME) {
        Reduce_Value_Q_NAME = reduce_Value_Q_NAME;
        properties.setProperty("Reduce_Value_Q_NAME",reduce_Value_Q_NAME);
    }

    public void setKeyed_Value_Q_NAME(String keyed_Value_Q_NAME) {
        Keyed_Value_Q_NAME = keyed_Value_Q_NAME;
        properties.setProperty("Keyed_Value_Q_NAME",keyed_Value_Q_NAME);
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setStatekey(String statekey) {
        this.statekey = statekey;
        properties.setProperty("statekey",statekey);
    }

}
