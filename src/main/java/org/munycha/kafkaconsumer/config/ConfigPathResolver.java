package org.munycha.kafkaconsumer.config;

public final class ConfigPathResolver {

    private ConfigPathResolver() {
    }

    public static String resolve(
            String[] args,
            String envKey,
            String sysPropKey,
            String defaultPath
    ) {

        // 1. CLI arguments
        String cliPath = resolveFromArgs(args);
        if (cliPath != null) {
            return cliPath;
        }

        // 2. Environment variable
        String envPath = System.getenv(envKey);
        if (envPath != null && !envPath.isBlank()) {
            return envPath;
        }

        // 3. JVM system property
        String sysPropPath = System.getProperty(sysPropKey);
        if (sysPropPath != null && !sysPropPath.isBlank()) {
            return sysPropPath;
        }

        // 4. Default
        return defaultPath;
    }

    private static String resolveFromArgs(String[] args) {

        if (args == null) {
            return null;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.startsWith("--config=")) {
                return arg.substring("--config=".length());
            }

            if (arg.equals("--config") && i + 1 < args.length) {
                return args[i + 1];
            }

            if (!arg.startsWith("-") && !arg.isBlank()) {
                return arg;
            }
        }

        return null;
    }
}
