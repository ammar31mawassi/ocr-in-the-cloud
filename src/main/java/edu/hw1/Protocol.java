package edu.hw1;

import java.util.List;

final class Protocol {
    enum Command { SUBMIT, TERMINATE }
    enum Status { ACCEPTED, PROCESSING, COMPLETED, REJECTED, FAILED }

    record Request(Command command, String workId, String inputKey, String replyQueue, int filesPerWorker) {}
    record Task(String jobId, String url) {}
    record Result(String jobId, String text, String error, String textKey) {
        Result(String jobId, String text, String error) {
            this(jobId, text, error, null);
        }
    }
    record Reply(String workId, Status status, String summaryKey, String detail) {}
    record Job(Request request, List<String> urls) {
        Task task(int index) {
            return new Task(request.workId() + ":" + (index + 1), urls.get(index));
        }
    }
    record Row(String url, String text, String error) {}
}
