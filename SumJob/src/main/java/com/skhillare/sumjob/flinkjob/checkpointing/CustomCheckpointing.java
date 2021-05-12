package com.skhillare.sumjob.flinkjob.checkpointing;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.util.concurrent.TimeUnit;

public class CustomCheckpointing {
    public static CheckpointConfig setCheckpointing(CheckpointConfig configuration,long cpInterval) {
        //Checkpointing configuration
        if (cpInterval > 0) {
            configuration.setCheckpointInterval(cpInterval);
            configuration.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            configuration.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
            configuration.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        return configuration;
    }

}
