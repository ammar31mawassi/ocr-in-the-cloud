package edu.hw1;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.s3.model.S3Exception;

final class Ec2Fleet implements AutoCloseable {
    private final AwsIo aws;
    private final Ec2Client ec2;
    private final int limit;
    private final String self;
    private String publishedStatus;
    private int publishedSlots = -1;

    static Ec2Fleet open(AwsIo aws) {
        return aws.settings.local() ? null : new Ec2Fleet(aws);
    }

    private Ec2Fleet(AwsIo aws) {
        this.aws = aws;
        self = System.getenv("HW1_INSTANCE_ID");
        limit = Math.min(19, App.positive(Settings.env("HW1_INSTANCE_LIMIT", "19")));
        ec2 = Ec2Client.builder().region(aws.settings.region()).build();
    }

    String findOrStartManager(int capacity, Set<String> rejected) throws InterruptedException {
        long start = System.nanoTime();
        while (System.nanoTime() - start < Duration.ofMinutes(8).toNanos()) {
            List<Instance> managers = instances(tagFilter("Project", aws.settings.prefix()),
                    tagFilter("Role", "Manager"), states("pending", "running"));
            for (Instance manager : managers) {
                String id = value(manager, "ManagerId");
                if (!rejected.contains(id) && "READY".equals(value(manager, "Status"))
                        && Integer.parseInt(value(manager, "AvailableSlots")) > 0) return id;
            }
            if (managers.stream().noneMatch(i -> "STARTING".equals(value(i, "Status")))) {
                String id = UUID.randomUUID().toString();
                aws.managerQueues(id);
                withLaunchLock(() -> {
                    if (room() > 0) launch(id, true, capacity, 1);
                });
            }
            Thread.sleep(4000);
        }
        throw new IllegalStateException("No Manager became available. Check EC2 status and bootstrap logs.");
    }

    void ensureWorkers(String manager, int desired) {
        if (desired < 1) return;
        if (workers(manager).size() >= desired) return;
        withLaunchLock(() -> {
            int count = Math.min(desired - workers(manager).size(), room());
            if (count > 0) launch(manager, false, 0, count);
        });
    }

    private List<Instance> workers(String manager) {
        return instances(tagFilter("Project", aws.settings.prefix()), tagFilter("ManagerId", manager),
                tagFilter("Role", "Worker"), states("pending", "running"));
    }

    void terminateWorkers(String manager) {
        List<String> ids = workers(manager).stream().map(Instance::instanceId).toList();
        if (!ids.isEmpty()) ec2.terminateInstances(r -> r.instanceIds(ids));
    }

    void terminateSelf() {
        ec2.terminateInstances(r -> r.instanceIds(self));
    }

    void updateSelf(String status, int slots) {
        if (status.equals(publishedStatus) && slots == publishedSlots) return;
        ec2.createTags(r -> r.resources(self).tags(tag("Status", status),
                tag("AvailableSlots", Integer.toString(slots))));
        publishedStatus = status;
        publishedSlots = slots;
    }

    private int room() {
        return Math.max(0, limit - instances(states("pending", "running", "stopping")).size());
    }

    private void withLaunchLock(Runnable launch) {
        // Serialize EC2 capacity checks for processes using this deployment prefix.
        String key = aws.settings.key("ec2-launch.lock");
        try {
            aws.s3.putObject(r -> r.bucket(aws.settings.bucket()).key(key).ifNoneMatch("*"),
                    RequestBody.fromString(UUID.randomUUID().toString()));
        } catch (S3Exception e) {
            if (e.statusCode() == 412 || e.statusCode() == 409) return;
            throw e;
        }
        try {
            launch.run();
        } finally {
            aws.s3.deleteObject(r -> r.bucket(aws.settings.bucket()).key(key));
        }
    }

    private void launch(String id, boolean manager, int capacity, int count) {
        String role = manager ? "Manager" : "Worker";
        String type = manager ? Settings.env("HW1_MANAGER_TYPE", "t3.micro")
                : Settings.env("HW1_WORKER_TYPE", "t2.nano");
        List<Tag> tags = List.of(tag("Name", aws.settings.prefix() + "-" + role),
                tag("Project", aws.settings.prefix()), tag("Role", role), tag("ManagerId", id),
                tag("Status", "STARTING"), tag("AvailableSlots", "0"));
        var launched = ec2.runInstances(r -> {
            r.imageId(Settings.required("HW1_AMI")).instanceType(type)
                    .minCount(1).maxCount(count).clientToken(UUID.randomUUID().toString())
                    .metadataOptions(m -> m.httpTokens("required"))
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .name(Settings.required("HW1_INSTANCE_PROFILE")).build())
                    .userData(Base64.getEncoder().encodeToString(
                            bootstrap(role, id, capacity).getBytes(StandardCharsets.UTF_8)))
                    .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(tags).build());
            if (Settings.env("HW1_SUBNET", null) != null) r.subnetId(Settings.required("HW1_SUBNET"));
            if (Settings.env("HW1_SECURITY_GROUP", null) != null) {
                r.securityGroupIds(Settings.required("HW1_SECURITY_GROUP"));
            }
        }).instances();
        List<String> ids = launched.stream().map(Instance::instanceId).toList();
        System.out.println(java.time.Instant.now() + " Launched " + role + " " + ids);
        ec2.waiter().waitUntilInstanceExists(r -> r.instanceIds(ids),
                c -> c.waitTimeout(Duration.ofSeconds(45))).matched().response().orElseThrow(
                        () -> new IllegalStateException("Could not confirm EC2 launch " + ids));
    }

    private String bootstrap(String role, String id, int capacity) {
        StringBuilder script = new StringBuilder("""
                #!/bin/bash
                set -euo pipefail
                echo "$(date -u +%FT%TZ) Bootstrap started"
                mkdir -p /opt/ocr/tessdata
                cd /opt/ocr
                """);
        export(script, "AWS_REGION", aws.settings.region().id());
        export(script, "HW1_BUCKET", aws.settings.bucket());
        export(script, "HW1_PREFIX", aws.settings.prefix());
        export(script, "HW1_INSTANCE_LIMIT", Integer.toString(limit));
        export(script, "OMP_THREAD_LIMIT", "1");
        if (role.equals("Worker")) export(script, "HW1_TESSERACT_BIN", "tesseract");
        for (String key : List.of("HW1_AMI", "HW1_INSTANCE_PROFILE", "HW1_SUBNET", "HW1_SECURITY_GROUP",
                "HW1_MANAGER_TYPE", "HW1_WORKER_TYPE", "HW1_ARTIFACT_PREFIX")) {
            if (Settings.env(key, null) != null) export(script, key, Settings.required(key));
        }
        String artifacts = Settings.env("HW1_ARTIFACT_PREFIX", aws.settings.key("artifacts"));
        script.append("""
                token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
                export HW1_INSTANCE_ID=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
                """);
        if (Boolean.parseBoolean(Settings.env("HW1_BOOT_INSTALL", "false"))) {
            export(script, "HW1_BOOT_INSTALL", "true");
            script.append("""
                    export DEBIAN_FRONTEND=noninteractive
                    apt-get update
                    apt-get install -y openjdk-17-jre-headless tesseract-ocr unzip
                    if ! command -v aws >/dev/null; then
                      curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
                      unzip -qo /tmp/awscliv2.zip -d /tmp/ocr-awscli
                      /tmp/ocr-awscli/aws/install
                    fi
                    """);
        }
        String s3 = "s3://" + aws.settings.bucket() + "/" + artifacts + "/";
        String jar = role.equals("Worker") ? "ocr-worker.jar" : "ocr-cloud.jar";
        script.append("aws s3 cp ").append(quote(s3 + jar)).append(" app.jar --only-show-errors\n");
        if (role.equals("Worker")) {
            script.append("aws s3 cp ").append(quote(s3 + "eng.traineddata")).append(" tessdata/eng.traineddata\n");
        }
        script.append("echo \"$(date -u +%FT%TZ) Artifacts downloaded, starting Java\"\n");
        script.append("exec java -Djava.awt.headless=true -Xmx")
                .append(role.equals("Worker") ? "160m" : "384m")
                .append(" -cp app.jar edu.hw1.").append(role).append(" ").append(quote(id));
        if (role.equals("Manager")) script.append(" ").append(capacity);
        return script.append("\n").toString();
    }

    private static void export(StringBuilder script, String key, String value) {
        script.append("export ").append(key).append("=").append(quote(value)).append("\n");
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private List<Instance> instances(Filter... filters) {
        return ec2.describeInstancesPaginator(r -> r.filters(filters)).reservations().stream()
                .flatMap(reservation -> reservation.instances().stream()).toList();
    }

    private static Filter tagFilter(String name, String value) {
        return Filter.builder().name("tag:" + name).values(value).build();
    }

    private static Filter states(String... states) {
        return Filter.builder().name("instance-state-name").values(states).build();
    }

    private static Tag tag(String key, String value) {
        return Tag.builder().key(key).value(value).build();
    }

    private static String value(Instance instance, String key) {
        return instance.tags().stream().filter(tag -> tag.key().equals(key)).map(Tag::value)
                .findFirst().orElse("");
    }

    @Override
    public void close() {
        ec2.close();
    }
}
