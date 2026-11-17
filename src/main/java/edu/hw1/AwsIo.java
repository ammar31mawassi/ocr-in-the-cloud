package edu.hw1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

final class AwsIo implements AutoCloseable {
    record Queues(String requests, String tasks, String results) {}

    private final ObjectMapper json = new ObjectMapper();
    final Settings settings;
    final S3Client s3;
    private final SqsClient sqs;

    AwsIo(Settings settings) {
        this.settings = settings;
        var storage = S3Client.builder().region(settings.region());
        var queues = SqsClient.builder().region(settings.region())
                .overrideConfiguration(c -> c.apiCallAttemptTimeout(Duration.ofSeconds(30))
                        .apiCallTimeout(Duration.ofSeconds(45)));
        if (settings.local() && System.getenv("HW1_ENDPOINT") != null) {
            URI endpoint = URI.create(Settings.required("HW1_ENDPOINT"));
            storage.endpointOverride(endpoint).forcePathStyle(true);
            queues.endpointOverride(endpoint);
        }
        s3 = storage.build();
        sqs = queues.build();
    }

    Queues managerQueues(String id) {
        return new Queues(queue(id + "-requests"), queue(id + "-tasks"), queue(id + "-results"));
    }

    String queue(String suffix) {
        return sqs.createQueue(r -> r.queueName(settings.prefix() + "-" + suffix)).queueUrl();
    }

    void send(String queue, Object value) {
        sqs.sendMessage(r -> r.queueUrl(queue).messageBody(encode(value)));
    }

    List<Message> receive(String queue, int count, int wait, int visibility) {
        return sqs.receiveMessage(r -> r.queueUrl(queue).maxNumberOfMessages(count)
                .waitTimeSeconds(wait).visibilityTimeout(visibility)).messages();
    }

    void visibility(String queue, Message message, int seconds) {
        sqs.changeMessageVisibility(r -> r.queueUrl(queue).receiptHandle(message.receiptHandle())
                .visibilityTimeout(seconds));
    }

    void ack(String queue, Message message) {
        sqs.deleteMessage(r -> r.queueUrl(queue).receiptHandle(message.receiptHandle()));
    }

    void deleteQueue(String queue) {
        sqs.deleteQueue(r -> r.queueUrl(queue));
    }

    void write(String key, Object value) {
        writeText(key, encode(value));
    }

    void writeText(String key, String value) {
        s3.putObject(r -> r.bucket(settings.bucket()).key(key), RequestBody.fromString(value));
    }

    String readText(String key) {
        return s3.getObjectAsBytes(r -> r.bucket(settings.bucket()).key(key)).asUtf8String();
    }

    <T> T read(String key, Class<T> type) {
        return decode(readText(key), type);
    }

    <T> T readIfPresent(String key, Class<T> type) {
        try {
            return read(key, type);
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    List<String> keys(String prefix) {
        return s3.listObjectsV2Paginator(r -> r.bucket(settings.bucket()).prefix(prefix))
                .contents().stream().map(S3Object::key).toList();
    }

    <T> T decode(String value, Class<T> type) {
        try {
            return json.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid " + type.getSimpleName(), e);
        }
    }

    private String encode(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to encode " + value.getClass().getSimpleName(), e);
        }
    }

    @Override
    public void close() {
        sqs.close();
        s3.close();
    }
}
