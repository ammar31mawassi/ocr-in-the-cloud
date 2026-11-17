package edu.hw1;

import static edu.hw1.Protocol.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.model.Message;

public final class Manager {
    private final AwsIo aws;
    private final AwsIo.Queues queues;
    private final JobStore store;
    private final Ec2Fleet fleet;
    private final String managerId;
    private final int capacity;
    private final Map<String, ActiveJob> active = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> resultFailure = new AtomicReference<>();
    private volatile boolean draining;
    private volatile boolean running = true;

    private static final class ActiveJob {
        final Job job;
        final Map<String, Result> resultsByUrl = new HashMap<>();
        final int uniqueImages;
        final ArrayDeque<Task> pendingDispatch = new ArrayDeque<>();
        volatile int finished;
        volatile boolean dispatched;

        ActiveJob(Job job, Map<String, Result> results) {
            this.job = job;
            var firstTaskByUrl = new java.util.LinkedHashMap<String, Task>();
            for (int index = 0; index < job.urls().size(); index++) {
                Task task = job.task(index);
                firstTaskByUrl.putIfAbsent(task.url(), task);
                Result saved = results.get(task.jobId());
                if (saved != null) resultsByUrl.putIfAbsent(task.url(), saved);
            }
            uniqueImages = firstTaskByUrl.size();
            finished = resultsByUrl.size();
            // Older runs may have saved a result under any occurrence of the URL.
            firstTaskByUrl.forEach((url, task) -> {
                if (!resultsByUrl.containsKey(url)) pendingDispatch.addLast(task);
            });
            dispatched = pendingDispatch.isEmpty();
        }
    }

    Manager(AwsIo aws, Ec2Fleet fleet, String id, int capacity) {
        this.aws = aws;
        this.fleet = fleet;
        managerId = id;
        this.capacity = capacity;
        queues = aws.managerQueues(id);
        store = new JobStore(aws, id);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: Manager manager-id capacity");
        Settings settings = Settings.load();
        try (AwsIo aws = new AwsIo(settings); Ec2Fleet fleet = Ec2Fleet.open(aws)) {
            new Manager(aws, fleet, args[0], App.positive(args[1])).run();
        }
    }

    private void run() throws InterruptedException {
        draining = Boolean.TRUE.equals(aws.readIfPresent(
                aws.settings.key("managers/" + managerId + "/draining.json"), Boolean.class));
        for (Job job : store.unfinished()) {
            String id = job.request().workId();
            active.put(id, new ActiveJob(job, store.results(id)));
        }
        updateTags();
        Thread collector = new Thread(() -> {
            try {
                collectResults();
            } catch (RuntimeException e) {
                resultFailure.set(e);
            }
        }, "worker-results");
        collector.start();
        var provisioning = Executors.newSingleThreadScheduledExecutor();
        try {
            if (fleet != null) {
                // EC2 calls can wait while the request thread keeps dispatching tasks.
                provisioning.scheduleWithFixedDelay(() -> retryAws(this::scale), 0, 2, TimeUnit.SECONDS);
            }
            System.out.println(java.time.Instant.now() + " Manager " + managerId + " ready");
            while (!draining || !active.isEmpty()) {
                if (resultFailure.get() != null) throw resultFailure.get();
                retryAws(() -> {
                    poll(queues.requests(), 1, this::handleRequest);
                    for (ActiveJob work : active.values()) dispatch(work);
                    updateTags();
                });
            }
        } finally {
            running = false;
            provisioning.shutdown();
            try {
                while (!provisioning.awaitTermination(5, TimeUnit.SECONDS)) {
                    // Wait for any launch in progress before finding the Workers to terminate.
                }
            } finally {
                collector.join();
            }
        }
        updateTags();
        if (fleet != null) fleet.terminateWorkers(managerId);
        aws.deleteQueue(queues.tasks());
        aws.deleteQueue(queues.results());
        aws.deleteQueue(queues.requests());
        if (fleet != null) fleet.terminateSelf();
        System.out.println("Manager " + managerId + " drained");
    }

    private void handleRequest(Message message) {
        Request request = aws.decode(message.body(), Request.class);
        String id = request.workId();
        if (request.command() == Command.TERMINATE) {
            aws.write(aws.settings.key("managers/" + managerId + "/draining.json"), true);
            draining = true;
            updateTags();
        } else if (active.containsKey(id)) {
            reply(request, Status.ACCEPTED, null, null);
        } else if (store.status(id) == Status.COMPLETED) {
            reply(request, Status.COMPLETED, store.summaryKey(id), null);
        } else if (draining || active.size() >= capacity) {
            reply(request, Status.REJECTED, null, draining ? "Manager is draining" : "Manager is full");
        } else {
            Job job = store.job(id);
            if (job == null) {
                List<String> urls = aws.readText(request.inputKey()).lines()
                        .map(String::strip).filter(url -> !url.isEmpty()).toList();
                job = new Job(request, urls);
                store.save(job);
            }
            store.status(id, Status.ACCEPTED);
            active.put(id, new ActiveJob(job, store.results(id)));
            reply(request, Status.ACCEPTED, null, null);
            System.out.println("Accepted " + id + " (" + job.urls().size() + " images)");
        }
        aws.ack(queues.requests(), message);
    }

    private void dispatch(ActiveJob work) {
        if (work.dispatched) return;
        store.status(work.job.request().workId(), Status.PROCESSING);
        for (int sent = 0; sent < 10 && !work.pendingDispatch.isEmpty(); sent++) {
            aws.send(queues.tasks(), work.pendingDispatch.getFirst());
            work.pendingDispatch.removeFirst();
        }
        work.dispatched = work.pendingDispatch.isEmpty();
    }

    private void collectResults() {
        while (running) {
            retryAws(() -> {
                poll(queues.results(), 10, this::handleResult);
                for (ActiveJob work : active.values()) {
                    if (work.dispatched && work.finished == work.uniqueImages) complete(work);
                }
            });
        }
    }

    private void handleResult(Message message) {
        Result result = aws.decode(message.body(), Result.class);
        String[] parts = result.jobId().split(":", -1);
        if (parts.length != 2 || !parts[1].matches("[1-9][0-9]{0,8}")) {
            throw new IllegalArgumentException("Invalid job ID");
        }
        ActiveJob work = active.get(parts[0]);
        int index = Integer.parseInt(parts[1]) - 1;
        if (work != null && index < work.job.urls().size()) {
            String url = work.job.urls().get(index);
            if (!work.resultsByUrl.containsKey(url)) {
                store.saveResult(parts[0], result);
                work.resultsByUrl.put(url, result);
                work.finished = work.resultsByUrl.size();
            }
        }
        aws.ack(queues.results(), message);
    }

    private void complete(ActiveJob work) {
        Request request = work.job.request();
        String id = request.workId();
        List<Row> rows = new ArrayList<>();
        Map<String, Row> rowsByUrl = new HashMap<>();
        for (String url : work.job.urls()) {
            rows.add(rowsByUrl.computeIfAbsent(url, key -> {
                Result result = work.resultsByUrl.get(key);
                String text = result.text();
                if (text == null && result.textKey() != null) text = aws.readText(result.textKey());
                return new Row(key, text, result.error());
            }));
        }
        // Publish the summary before notifying the client. A restart can safely send the reply again.
        String key = store.summaryKey(id);
        aws.write(key, rows);
        reply(request, Status.COMPLETED, key, null);
        store.status(id, Status.COMPLETED);
        active.remove(id, work);
        System.out.println("Completed " + id);
    }

    private void poll(String queue, int count, Consumer<Message> handler) {
        for (Message message : aws.receive(queue, count, 2, 120)) {
            try {
                handler.accept(message);
            } catch (IllegalArgumentException e) {
                System.err.println("Discarded malformed queue message: " + e.getMessage());
                aws.ack(queue, message);
            }
        }
    }

    private void reply(Request request, Status status, String key, String detail) {
        try {
            aws.send(request.replyQueue(), new Reply(request.workId(), status, key, detail));
        } catch (software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException e) {
            System.err.println("Reply queue was removed for " + request.workId());
        }
    }

    private void scale() {
        if (fleet == null) return;
        double share = active.values().stream().mapToDouble(work ->
                (double) (work.uniqueImages - work.finished) / work.job.request().filesPerWorker()).sum();
        fleet.ensureWorkers(managerId, Math.max(draining ? 0 : 1, (int) Math.ceil(share)));
    }

    private void updateTags() {
        if (fleet != null) fleet.updateSelf(draining ? "DRAINING" : "READY",
                draining ? 0 : Math.max(0, capacity - active.size()));
    }

    private static void retryAws(Runnable action) {
        try {
            action.run();
        } catch (SdkException e) {
            System.err.println("Manager will retry: " + e.getMessage());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
