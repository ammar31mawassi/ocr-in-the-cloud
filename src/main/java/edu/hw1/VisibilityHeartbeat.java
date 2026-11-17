package edu.hw1;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.sqs.model.Message;

final class VisibilityHeartbeat implements AutoCloseable {
    private final AwsIo aws;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

    VisibilityHeartbeat(AwsIo aws) {
        this.aws = aws;
    }

    ScheduledFuture<?> start(String queue, Message message) {
        // Keep long OCR operations hidden while this worker is alive.
        return timer.scheduleAtFixedRate(() -> {
            try {
                aws.visibility(queue, message, 90);
            } catch (RuntimeException e) {
                System.err.println("Visibility renewal failed: " + e.getMessage());
            }
        }, 25, 25, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
