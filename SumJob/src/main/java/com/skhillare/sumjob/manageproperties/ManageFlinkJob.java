package com.skhillare.sumjob.manageproperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ManageFlinkJob {
    //Manage flinkjob.properties
    private Integer Port;
    private String hostname;
    private long cpInterval;
    private final String filename="flinkjob.properties";
    private Integer parallelism;
    private Properties properties;

    public ManageFlinkJob(){
        properties=new Properties();
        try {
            InputStream in = ManageFixedProperties.class.getClassLoader().getResourceAsStream(filename);
            properties.load(in);
            this.properties=properties;
            this.cpInterval= Long.parseLong(properties.getProperty("cpInterval"));
            this.hostname=properties.getProperty("hostname");
            this.Port= Integer.valueOf(properties.getProperty("port"));
            this.parallelism= Integer.valueOf(properties.getProperty("parallelism"));
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public long getCpInterval() {
        return cpInterval;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return Port;
    }

    public Integer getParallelism() { return parallelism; }


    public void setPort(Integer port) {
        Port = port;
        properties.setProperty("port", String.valueOf(port));
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
        properties.setProperty("hostname",hostname);
    }

    public void setCpInterval(long cpInterval) {
        this.cpInterval = cpInterval;
        properties.setProperty("cpInterval", String.valueOf(cpInterval));
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        properties.setProperty("parallelism",String.valueOf(parallelism));
    }

}
