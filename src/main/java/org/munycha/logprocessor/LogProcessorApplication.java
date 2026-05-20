package org.munycha.logprocessor;

import org.munycha.logprocessor.bootstrap.ApplicationBootstrap;
import org.munycha.logprocessor.bootstrap.RunningApplication;
import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.ConfigLoader;
import org.munycha.logprocessor.config.ConfigPathResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class LogProcessorApplication {

    private static final Logger log = LoggerFactory.getLogger(LogProcessorApplication.class);

    public static void main(String[] args) throws Exception {
        String configPath = ConfigPathResolver.resolve(
                args,
                "CONSUMER_CONFIG",            // ENV var
                "consumer.config",            // JVM system property
                "config/consumer_config.json" // classpath default
        );
        log.info("Using config path: {}", configPath);

        AppConfig config = new ConfigLoader(configPath).load();
        RunningApplication app = ApplicationBootstrap.start(config);

        Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
        new CountDownLatch(1).await();
    }
}
