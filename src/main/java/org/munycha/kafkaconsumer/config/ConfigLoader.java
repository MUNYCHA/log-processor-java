package org.munycha.kafkaconsumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;

public class ConfigLoader {

    private final String filePath;

    public ConfigLoader(String filePath) {
        this.filePath = filePath;
    }

    public AppConfig load() throws IOException {
        try (InputStream inputStream = loadConfigFile(filePath)) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(inputStream, AppConfig.class);
        }
    }

    private InputStream loadConfigFile(String filePath) throws FileNotFoundException {

        File externalFile = new File(filePath);

        if (externalFile.isFile()) {
            return new FileInputStream(externalFile);
        }

        InputStream internalStream =
                getClass().getClassLoader().getResourceAsStream(filePath);

        if (internalStream != null) {
            System.out.println(
                    "[ConfigLoader] External config not found, using INTERNAL config: " + filePath
            );
            return internalStream;
        }

        throw new FileNotFoundException(
                "Config file not found (external or classpath): " + filePath
        );
    }
}

