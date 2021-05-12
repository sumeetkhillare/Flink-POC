package com.skhillare.wordcount.initialization.userinput;

import com.skhillare.wordcount.manageproperties.ManageFlinkJob;
import com.skhillare.wordcount.manageproperties.ManageQueryJob;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.concurrent.TimeUnit;

public class UserInput {
    //User Input for flink job
    public static ManageFlinkJob FlinkJobInput(String[] args){
        final String hostname;
        final Integer port;
        final Integer parallelism;
        ManageFlinkJob manageProperties=new ManageFlinkJob();
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
            parallelism = params.has("parallelism") ? params.getInt("parallelism") : 1;
            long cpInterval = params.getLong("checkpoint", TimeUnit.MINUTES.toMillis(1));
            manageProperties.setCpInterval(cpInterval);
            manageProperties.setHostname(hostname);
            manageProperties.setPort(port);
            manageProperties.setParallelism(parallelism);
            return manageProperties;
        }catch (Exception e) {
            System.err.println(
                    "No port specified. Please specify --hostname <hostname> --port <port> --checkpoint <time> --parallelism <parallel task number>, where hostname (localhost by default) "
                            + "and port is the address of the text server "+ "and chekpoint is the time for checkpointing, parallelism is for setting parallelism");
            System.err.println(
                    "To start a simple server, run 'netcat -l <port>' and "
                            + "type the numbers into the command line");
            System.exit(0);
        }
        return manageProperties;
    }
    //User Input for queryable job
    public static ManageQueryJob QueryInput(String[] args){
        ManageQueryJob manageQueryJob=new ManageQueryJob();
        String query_hostname;
        Integer query_port;
        String jobId;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            query_hostname = params.has("hostname") ? params.get("hostname") : "127.0.1.1";
            query_port = params.has("port") ? params.getInt("port") : 9069;
            jobId = params.get("jobid");
            manageQueryJob.setJobId(jobId);
            manageQueryJob.setQuery_hostname(query_hostname);
            manageQueryJob.setQuery_port(query_port);
            return manageQueryJob;
        }catch (Exception e) {
            System.err.println("No jobid specified. Please specify jobid using -jobid <jobid>");
            System.exit(0);
        }
        return manageQueryJob;
    }
}
