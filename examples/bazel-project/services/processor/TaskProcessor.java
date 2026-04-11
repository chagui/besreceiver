package com.taskforge.processor;

import com.taskforge.proto.TaskProto.Task;
import com.taskforge.proto.TaskProto.TaskResult;
import com.taskforge.proto.TaskProto.Priority;
import com.taskforge.proto.TaskProto.Status;

import java.util.ArrayList;
import java.util.List;

/** Processes a batch of tasks and produces results. */
public final class TaskProcessor {

    public static void main(String[] args) {
        List<Task> batch = createSampleBatch();
        System.out.println("TaskForge Processor: processing " + batch.size() + " tasks");

        List<TaskResult> results = new ArrayList<>();
        for (Task task : batch) {
            TaskResult result = process(task);
            results.add(result);
            System.out.printf("  [%s] %s -> %s (%dms)%n",
                    task.getPriority(), task.getName(),
                    result.getStatus(), result.getDurationMs());
        }

        long completed = results.stream()
                .filter(r -> r.getStatus() == Status.COMPLETED)
                .count();
        long failed = results.stream()
                .filter(r -> r.getStatus() == Status.FAILED)
                .count();

        System.out.printf("Batch complete: %d/%d succeeded, %d failed%n",
                completed, batch.size(), failed);
    }

    static TaskResult process(Task task) {
        long duration;
        Status status;

        switch (task.getPriority()) {
            case CRITICAL:
                duration = 500;
                status = Status.COMPLETED;
                break;
            case HIGH:
                duration = 200;
                status = Status.COMPLETED;
                break;
            case MEDIUM:
                duration = 100;
                status = Status.COMPLETED;
                break;
            case LOW:
                duration = 50;
                status = task.getName().contains("flaky")
                        ? Status.FAILED : Status.COMPLETED;
                break;
            default:
                duration = 10;
                status = Status.FAILED;
                break;
        }

        return TaskResult.newBuilder()
                .setTaskId(task.getId())
                .setStatus(status)
                .setOutput("processed by TaskForge")
                .setDurationMs(duration)
                .build();
    }

    static List<Task> createSampleBatch() {
        List<Task> tasks = new ArrayList<>();
        tasks.add(Task.newBuilder()
                .setId("task-001").setName("deploy-api")
                .setPriority(Priority.CRITICAL).setStatus(Status.PENDING)
                .build());
        tasks.add(Task.newBuilder()
                .setId("task-002").setName("run-migrations")
                .setPriority(Priority.HIGH).setStatus(Status.PENDING)
                .build());
        tasks.add(Task.newBuilder()
                .setId("task-003").setName("send-notifications")
                .setPriority(Priority.MEDIUM).setStatus(Status.PENDING)
                .build());
        tasks.add(Task.newBuilder()
                .setId("task-004").setName("cleanup-logs")
                .setPriority(Priority.LOW).setStatus(Status.PENDING)
                .build());
        tasks.add(Task.newBuilder()
                .setId("task-005").setName("flaky-job")
                .setPriority(Priority.LOW).setStatus(Status.PENDING)
                .build());
        return tasks;
    }
}
