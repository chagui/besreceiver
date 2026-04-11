package com.taskforge.processor;

import com.taskforge.proto.TaskProto.Task;
import com.taskforge.proto.TaskProto.TaskResult;
import com.taskforge.proto.TaskProto.Priority;
import com.taskforge.proto.TaskProto.Status;

public final class TaskProcessorTest {

    public static void main(String[] args) {
        testCriticalTaskCompletes();
        testLowPriorityFlakyFails();
        testHighPriorityCompletes();
        System.out.println("PASSED: all processor tests");
    }

    static void testCriticalTaskCompletes() {
        Task task = Task.newBuilder()
                .setId("t1").setName("critical-job")
                .setPriority(Priority.CRITICAL).setStatus(Status.PENDING)
                .build();
        TaskResult result = TaskProcessor.process(task);
        assertEqual(result.getStatus(), Status.COMPLETED, "critical task should complete");
        assertTrue(result.getDurationMs() == 500, "critical duration should be 500ms");
    }

    static void testLowPriorityFlakyFails() {
        Task task = Task.newBuilder()
                .setId("t2").setName("flaky-job")
                .setPriority(Priority.LOW).setStatus(Status.PENDING)
                .build();
        TaskResult result = TaskProcessor.process(task);
        assertEqual(result.getStatus(), Status.FAILED, "flaky low-priority task should fail");
    }

    static void testHighPriorityCompletes() {
        Task task = Task.newBuilder()
                .setId("t3").setName("important-job")
                .setPriority(Priority.HIGH).setStatus(Status.PENDING)
                .build();
        TaskResult result = TaskProcessor.process(task);
        assertEqual(result.getStatus(), Status.COMPLETED, "high-priority task should complete");
    }

    static void assertEqual(Object actual, Object expected, String msg) {
        if (!actual.equals(expected)) {
            throw new AssertionError(msg + ": expected " + expected + ", got " + actual);
        }
    }

    static void assertTrue(boolean condition, String msg) {
        if (!condition) {
            throw new AssertionError(msg);
        }
    }
}
