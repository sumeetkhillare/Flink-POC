package com.skhillare.wordcount.manageproperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ManageFixedProperties {
    //Manage fixedvalues.properties
    private String Value_Q_NAME;
    private Properties properties;
    private String filename="fixedvalues.properties";
    public ManageFixedProperties(){
        properties=new Properties();
        try {
            InputStream in = ManageFixedProperties.class.getClassLoader().getResourceAsStream(filename);
            properties.load(in);
            this.properties=properties;
            this.Value_Q_NAME=properties.getProperty("Value_Q_NAME");
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public String getValue_Q_NAME() {
        return Value_Q_NAME;
    }

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

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }


}
