package edu.hw1;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import static edu.hw1.Protocol.*;

public final class Worker {
    private static final int MAX_BYTES = 12 * 1024 * 1024;
    private static final long MAX_PIXELS = 12_000_000;
    private static final int MAX_REDIRECTS = 5;
    private final OcrService ocr;

    Worker(OcrService ocr) {
        this.ocr = ocr;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: Worker manager-id");
        Settings settings = Settings.load();
        try (AwsIo aws = new AwsIo(settings)) {
            new Worker(new OcrService(settings.tessdata())).run(aws, aws.managerQueues(args[0]));
        }
    }

    private void run(AwsIo aws, AwsIo.Queues queues) throws InterruptedException {
        System.out.println(java.time.Instant.now() + " Worker ready");
        boolean firstTask = true;
        try (VisibilityHeartbeat heartbeat = new VisibilityHeartbeat(aws)) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (var message : aws.receive(queues.tasks(), 1, 10, 90)) {
                        Task task;
                        try {
                            task = aws.decode(message.body(), Task.class);
                        } catch (IllegalArgumentException e) {
                            System.err.println("Discarded malformed task: " + e.getMessage());
                            aws.ack(queues.tasks(), message);
                            continue;
                        }
                        var renewal = heartbeat.start(queues.tasks(), message);
                        try {
                            if (firstTask) {
                                System.out.println(java.time.Instant.now() + " First task received: " + task.jobId());
                                firstTask = false;
                            }
                            System.out.println("Processing " + task.jobId());
                            Result result = process(task);
                            if (result.text() != null) {
                                String key = aws.settings.key("text/" + task.jobId().replace(':', '/')
                                        + "/" + java.util.UUID.randomUUID() + ".txt");
                                aws.writeText(key, result.text());
                                String inline = result.text().length() < 16_000 ? result.text() : null;
                                result = new Result(task.jobId(), inline, null, key);
                            }
                            aws.send(queues.results(), result);
                            aws.ack(queues.tasks(), message);
                        } finally {
                            renewal.cancel(false);
                        }
                    }
                } catch (software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException e) {
                    System.out.println("Worker queue closed");
                    return;
                } catch (software.amazon.awssdk.core.exception.SdkException e) {
                    System.err.println("Worker will retry: " + e.getMessage());
                    Thread.sleep(2000);
                }
            }
        }
    }

    private Result process(Task task) {
        try {
            String text = ocr.extractText(download(task.url()));
            return new Result(task.jobId(), text, null);
        } catch (Exception e) {
            String detail = e instanceof UnknownHostException ? "Could not resolve image host: " + e.getMessage()
                    : e.getMessage();
            if (detail == null || detail.isBlank()) detail = e.getClass().getSimpleName();
            return new Result(task.jobId(), null, detail.substring(0, Math.min(240, detail.length())));
        }
    }

    static BufferedImage download(String url) throws IOException {
        URI uri = URI.create(url);
        for (int redirects = 0; ; redirects++) {
            if (uri.getHost() == null || (!"http".equalsIgnoreCase(uri.getScheme())
                    && !"https".equalsIgnoreCase(uri.getScheme()))) {
                throw new IOException("Expected an HTTP or HTTPS image URL");
            }
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setInstanceFollowRedirects(false);
            connection.setConnectTimeout(8000);
            connection.setReadTimeout(12000);
            try {
                int status = connection.getResponseCode();
                if (status == 200) return readImage(connection);
                if (status != 301 && status != 302 && status != 303 && status != 307 && status != 308) {
                    throw new IOException("Image server returned HTTP " + status);
                }
                if (redirects == MAX_REDIRECTS) throw new IOException("Too many image redirects");
                String location = connection.getHeaderField("Location");
                if (location == null || location.isBlank()) {
                    throw new IOException("Image redirect has no destination");
                }
                URI next = uri.resolve(location);
                if ("https".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(next.getScheme())) {
                    throw new IOException("Image redirect would leave HTTPS");
                }
                uri = next;
            } finally {
                connection.disconnect();
            }
        }
    }

    private static BufferedImage readImage(HttpURLConnection connection) throws IOException {
        Path temp = Files.createTempFile("ocr-image-", ".img");
        try {
            try (InputStream input = connection.getInputStream();
                 var output = Files.newOutputStream(temp)) {
                long length = input.transferTo(new BoundedOutput(output, MAX_BYTES));
                if (length == 0) throw new IOException("Image response is empty");
            }
            return decodeImage(temp);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static BufferedImage decodeImage(Path path) throws IOException {
        try (var stream = ImageIO.createImageInputStream(path.toFile())) {
            var readers = ImageIO.getImageReaders(stream);
            if (!readers.hasNext()) throw new IOException("Unsupported or unreadable image");
            ImageReader reader = readers.next();
            try {
                reader.setInput(stream);
                if ((long) reader.getWidth(0) * reader.getHeight(0) > MAX_PIXELS) {
                    throw new IOException("Image exceeds the pixel limit");
                }
                return reader.read(0);
            } finally {
                reader.dispose();
            }
        }
    }

    private static final class BoundedOutput extends java.io.FilterOutputStream {
        private int remaining;

        BoundedOutput(java.io.OutputStream output, int limit) {
            super(output);
            remaining = limit;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (length > remaining) throw new IOException("Image exceeds the download size limit");
            out.write(bytes, offset, length);
            remaining -= length;
        }
    }
}
