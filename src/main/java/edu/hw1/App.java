package edu.hw1;

import static edu.hw1.Protocol.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public final class App {
    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5 || (args.length == 5 && !args[4].equals("terminate"))) {
            throw new IllegalArgumentException(
                    "Usage: java -jar yourjar.jar inputFileName outputFileName manager-clients-n worker-files-n [terminate]");
        }
        int capacity = positive(args[2]);
        int ratio = positive(args[3]);
        String input = Files.readString(Path.of(args[0]));
        Settings settings = Settings.load();
        try (AwsIo aws = new AwsIo(settings); Ec2Fleet fleet = Ec2Fleet.open(aws)) {
            run(aws, fleet, input, Path.of(args[1]), capacity, ratio, args.length == 5);
        }
    }

    private static void run(AwsIo aws, Ec2Fleet fleet, String input, Path output,
                            int capacity, int ratio, boolean terminate) throws Exception {
        String id = UUID.randomUUID().toString();
        String inputKey = aws.settings.key("jobs/" + id + "/input.txt");
        aws.writeText(inputKey, input);
        String replies = aws.queue("reply-" + id);
        System.out.println("Submitted work ID " + id);
        try {
            Set<String> rejected = new HashSet<>();
            while (true) {
                String manager = fleet == null ? "local" : fleet.findOrStartManager(capacity, rejected);
                String requests = aws.queue(manager + "-requests");
                Request request = new Request(Command.SUBMIT, id, inputKey, replies, ratio);
                aws.send(requests, request);
                Reply reply = await(aws, replies, requests, request);
                if (reply.status() == Status.REJECTED) {
                    if (fleet == null) throw new IllegalStateException(reply.detail());
                    rejected.add(manager);
                    continue;
                }
                if (reply.status() != Status.COMPLETED) throw new IllegalStateException(reply.detail());
                String summary = aws.readText(reply.summaryKey());
                Files.writeString(output, HtmlReport.render(aws.decode(summary, Row[].class)));
                System.out.println("Wrote " + output.toAbsolutePath());
                if (terminate) {
                    aws.send(requests, new Request(Command.TERMINATE, id, null, null, 0));
                }
                return;
            }
        } finally {
            aws.deleteQueue(replies);
        }
    }

    private static Reply await(AwsIo aws, String replies, String requests, Request request) {
        long lastSend = System.nanoTime();
        long limit = java.time.Duration.ofMinutes(30).toNanos();
        long start = lastSend;
        while (System.nanoTime() - start < limit) {
            for (var message : aws.receive(replies, 1, 10, 60)) {
                Reply reply = aws.decode(message.body(), Reply.class);
                aws.ack(replies, message);
                if (reply.workId().equals(request.workId()) && reply.status() != Status.ACCEPTED) {
                    return reply;
                }
            }
            if (System.nanoTime() - lastSend >= java.time.Duration.ofSeconds(45).toNanos()) {
                // An unanswered submit may already be accepted. Retry the same Manager.
                aws.send(requests, request);
                lastSend = System.nanoTime();
            }
        }
        throw new IllegalStateException("Timed out waiting for work " + request.workId());
    }

    static int positive(String arg) {
        int number = Integer.parseInt(arg);
        if (number < 1) throw new IllegalArgumentException("Expected a positive integer");
        return number;
    }
}
