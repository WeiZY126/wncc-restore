package config;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/15
 * Description:
 */
public final class EnvConfig {
    private static final class streamExecutionEnvironment {
        private final StreamExecutionEnvironment env;
        public streamExecutionEnvironment(StreamExecutionEnvironment env) {
            this.env = env;
            env.setParallelism(1);
//            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, org.apache.flink.api.common.time.Time.milliseconds(10)));
//            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//            checkpointConfig.setMinPauseBetweenCheckpoints(10 * 1000L);
//            checkpointConfig.setTolerableCheckpointFailureNumber(10);
//            checkpointConfig.setCheckpointTimeout(60 * 1000L);
//            env.disableOperatorChaining();
            env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//            env.getCheckpointConfig().setCheckpointStorage("file:///D:/Program_Software/IDEAL/wncc/checkpoints");
        }
    }

    private static class envHolder {
        private static final streamExecutionEnvironment envHolder =
                new streamExecutionEnvironment(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public static StreamExecutionEnvironment getEnv() {
        return envHolder.envHolder.env;
    }
}