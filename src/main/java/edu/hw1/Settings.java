package edu.hw1;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import software.amazon.awssdk.regions.Region;

record Settings(Region region, String bucket, String prefix, boolean local, Path tessdata) {
    private static final Properties DEFAULTS = loadDefaults();

    static Settings load() {
        String profile = env("AWS_PROFILE", null);
        if (profile != null && System.getProperty("aws.profile") == null) {
            System.setProperty("aws.profile", profile);
        }
        String prefix = env("HW1_PREFIX", "ocr-hw1");
        if (!prefix.matches("[a-zA-Z0-9-]{1,20}")) {
            throw new IllegalArgumentException("HW1_PREFIX must contain 1 to 20 letters, digits or hyphens");
        }
        return new Settings(Region.of(env("AWS_REGION", "us-east-1")), required("HW1_BUCKET"), prefix,
                Boolean.parseBoolean(env("HW1_LOCAL", "false")), Path.of(env("HW1_TESSDATA", "tessdata")));
    }

    String key(String suffix) {
        return prefix + "/" + suffix;
    }

    static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, DEFAULTS.getProperty(name, fallback));
    }

    static String required(String name) {
        String value = env(name, null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set " + name + " in hw1.properties or in the environment");
        }
        return value;
    }

    private static Properties loadDefaults() {
        Properties defaults = new Properties();
        Path file = Path.of("hw1.properties");
        if (Files.exists(file)) {
            try (var input = Files.newInputStream(file)) {
                defaults.load(input);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not read " + file.toAbsolutePath(), e);
            }
        }
        return defaults;
    }
}
