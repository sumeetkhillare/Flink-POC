package com.skhillare.sumjob.manageproperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ManageQueryJob {
    //Manage queryjob.properties
    private Integer query_port;
    private String query_hostname;
    private Properties properties;
    private String filename="queryjob.properties";
    private String jobId;

    public ManageQueryJob(){
        properties=new Properties();
        try {
            InputStream in = ManageFixedProperties.class.getClassLoader().getResourceAsStream(filename);
            properties.load(in);
            this.properties=properties;
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String getJobId() { return jobId; }

    public Integer getQuery_port() {
        return query_port;
    }

    public String getQuery_hostname() {
        return query_hostname;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getFilename() {
        return filename;
    }

    public void setQuery_hostname(String query_hostname) {
        this.query_hostname = query_hostname;
        properties.setProperty("query_hostname",query_hostname);
    }
    public void setQuery_port(Integer query_port) {
        this.query_port = query_port;
        properties.setProperty("query_port", String.valueOf(query_port));
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
        properties.setProperty("jobId",jobId);
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

}
