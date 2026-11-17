package edu.hw1;

import static edu.hw1.Protocol.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class JobStore {
    private final AwsIo aws;
    private final String root;

    JobStore(AwsIo aws, String managerId) {
        this.aws = aws;
        root = aws.settings.key("managers/" + managerId + "/");
    }

    void save(Job job) {
        aws.write(folder(job.request().workId()) + "job.json", job);
    }

    void status(String id, Status status) {
        aws.write(folder(id) + "status.json", status);
    }

    Status status(String id) {
        return aws.readIfPresent(folder(id) + "status.json", Status.class);
    }

    Job job(String id) {
        return aws.readIfPresent(folder(id) + "job.json", Job.class);
    }

    List<Job> unfinished() {
        List<Job> jobs = new ArrayList<>();
        for (String key : aws.keys(root)) {
            if (key.endsWith("/job.json")) {
                Job job = aws.read(key, Job.class);
                if (status(job.request().workId()) != Status.COMPLETED) jobs.add(job);
            }
        }
        return jobs;
    }

    Map<String, Result> results(String id) {
        Map<String, Result> results = new HashMap<>();
        for (String key : aws.keys(folder(id) + "results/")) {
            Result result = aws.read(key, Result.class);
            results.put(result.jobId(), result);
        }
        return results;
    }

    void saveResult(String id, Result result) {
        String line = result.jobId().substring(id.length() + 1);
        aws.write(folder(id) + "results/" + line + ".json", result);
    }

    String summaryKey(String id) {
        return aws.settings.key("jobs/" + id + "/summary.json");
    }

    private String folder(String id) {
        return root + id + "/";
    }
}
