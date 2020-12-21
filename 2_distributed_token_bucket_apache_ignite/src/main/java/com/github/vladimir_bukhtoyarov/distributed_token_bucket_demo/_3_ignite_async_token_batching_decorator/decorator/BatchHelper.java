package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for batching.
 *
 * It is just a copy
 *
 * @param <T> Task type
 * @param <R> Task result type
 * @param <CT> Combined task type
 * @param <CR> Combined task result
 */
public class BatchHelper<T, R, CT, CR> {

    private static final Function UNSUPPORTED = new Function() {
        @Override
        public Object apply(Object o) {
            throw new UnsupportedOperationException();
        }
    };

    private static final Object NEED_TO_EXECUTE_NEXT_BATCH = new Object();
    private static final WaitingTask QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS = new WaitingTask(null);
    private static final WaitingTask QUEUE_EMPTY = new WaitingTask(null);

    private final Function<List<T>, CT> taskCombiner;
    private final Function<CT, CR> combinedTaskExecutor;
    private final Function<T, R> taskExecutor;
    private final Function<CT, CompletableFuture<CR>> asyncCombinedTaskExecutor;
    private final Function<T, CompletableFuture<R>> asyncTaskExecutor;
    private final Function<CR, List<R>> combinedResultSplitter;

    private final AtomicReference<WaitingTask> headReference = new AtomicReference<>(QUEUE_EMPTY);

    public static <T, R, CT, CR> BatchHelper<T, R, CT, CR> sync(
            Function<List<T>, CT> taskCombiner,
            Function<CT, CR> combinedTaskExecutor,
            Function<T, R> taskExecutor,
            Function<CR, List<R>> combinedResultSplitter) {
        return new BatchHelper<T, R, CT, CR>(taskCombiner, combinedTaskExecutor, taskExecutor, UNSUPPORTED, UNSUPPORTED, combinedResultSplitter);
    }

    public static <T, R, CT, CR> BatchHelper<T, R, CT, CR> sync(
            Function<List<T>, CT> taskCombiner,
            Function<CT, CR> combinedTaskExecutor,
            Function<CR, List<R>> combinedResultSplitter) {
        Function<T, R> taskExecutor = new Function<T, R>() {
            @Override
            public R apply(T task) {
                CT combinedTask = taskCombiner.apply(Collections.singletonList(task));
                CR combinedResult = combinedTaskExecutor.apply(combinedTask);
                List<R> results = combinedResultSplitter.apply(combinedResult);
                return results.get(0);
            }
        };
        return new BatchHelper<T, R, CT, CR>(taskCombiner, UNSUPPORTED, UNSUPPORTED, UNSUPPORTED, UNSUPPORTED, combinedResultSplitter);
    }

    public static <T, R, CT, CR> BatchHelper<T, R, CT, CR> async(
            Function<List<T>, CT> taskCombiner,
            Function<CT, CompletableFuture<CR>> asyncCombinedTaskExecutor,
            Function<T, CompletableFuture<R>> asyncTaskExecutor,
            Function<CR, List<R>> combinedResultSplitter) {
        return new BatchHelper<T, R, CT, CR>(taskCombiner, UNSUPPORTED, UNSUPPORTED, asyncCombinedTaskExecutor, asyncTaskExecutor, combinedResultSplitter);
    }

    public static <T, R, CT, CR> BatchHelper<T, R, CT, CR> async(
            Function<List<T>, CT> taskCombiner,
            Function<CT, CompletableFuture<CR>> asyncCombinedTaskExecutor,
            Function<CR, List<R>> combinedResultSplitter
    ) {
        Function<T, CompletableFuture<R>> asyncTaskExecutor = new Function<T, CompletableFuture<R>>() {
            @Override
            public CompletableFuture<R> apply(T task) {
                CT combinedTask = taskCombiner.apply(Collections.singletonList(task));
                CompletableFuture<CR> resultFuture = asyncCombinedTaskExecutor.apply(combinedTask);
                return resultFuture.thenApply((CR combinedResult) -> {
                    List<R> results = combinedResultSplitter.apply(combinedResult);
                    return results.get(0);
                });
            }
        };
        return new BatchHelper<T, R, CT, CR>(taskCombiner, UNSUPPORTED, UNSUPPORTED, asyncCombinedTaskExecutor, asyncTaskExecutor, combinedResultSplitter);
    }

    private BatchHelper(Function<List<T>, CT> taskCombiner,
                        Function<CT, CR> combinedTaskExecutor,
                        Function<T, R> taskExecutor,
                        Function<CT, CompletableFuture<CR>> asyncCombinedTaskExecutor,
                        Function<T, CompletableFuture<R>> asyncTaskExecutor,
                        Function<CR, List<R>> combinedResultSplitter) {
        this.taskCombiner = requireNonNull(taskCombiner);
        this.combinedTaskExecutor = requireNonNull(combinedTaskExecutor);
        this.taskExecutor = requireNonNull(taskExecutor);
        this.asyncCombinedTaskExecutor = requireNonNull(asyncCombinedTaskExecutor);
        this.asyncTaskExecutor = requireNonNull(asyncTaskExecutor);
        this.combinedResultSplitter = requireNonNull(combinedResultSplitter);
    }

    public R execute(T task) {
        WaitingTask<T, R> waitingNode = lockExclusivelyOrEnqueue(task);

        if (waitingNode == null) {
            try {
                return taskExecutor.apply(task);
            } finally {
                wakeupAnyThreadFromNextBatchOrFreeLock();
            }
        }

        R result = waitingNode.waitUninterruptedly();
        if (result != NEED_TO_EXECUTE_NEXT_BATCH) {
            // our future completed by another thread from current batch
            return result;
        }

        // current thread is responsible to execute the batch of commands
        try {
            return executeBatch(waitingNode);
        } finally {
            wakeupAnyThreadFromNextBatchOrFreeLock();
        }
    }

    public CompletableFuture<R> executeAsync(T task) {
        WaitingTask<T, R> waitingTask = lockExclusivelyOrEnqueue(task);

        if (waitingTask != null) {
            // there is another request is in progress, our request will be scheduled later
            return waitingTask.future;
        }

        try {
            return asyncTaskExecutor.apply(task)
                    .whenComplete((result, error) -> scheduleNextBatchAsync());
        } catch (Throwable error) {
            CompletableFuture<R> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(error);
            return failedFuture;
        }
    }

    private void scheduleNextBatchAsync() {
        List<WaitingTask<T, R>> waitingNodes = takeAllWaitingTasksOrFreeLock();
        if (waitingNodes.isEmpty()) {
            return;
        }

        try {
            List<T> commandsInBatch = new ArrayList<>(waitingNodes.size());
            for (WaitingTask<T, R> waitingNode : waitingNodes) {
                commandsInBatch.add(waitingNode.wrappedTask);
            }
            CT multiCommand = taskCombiner.apply(commandsInBatch);
            CompletableFuture<CR> combinedFuture = asyncCombinedTaskExecutor.apply(multiCommand);
            combinedFuture
                .whenComplete((multiResult, error) -> completeWaitingFutures(waitingNodes, multiResult, error))
                .whenComplete((multiResult, error) -> scheduleNextBatchAsync());
        } catch (Throwable e) {
            try {
                for (WaitingTask waitingNode : waitingNodes) {
                    waitingNode.future.completeExceptionally(e);
                }
            } finally {
                scheduleNextBatchAsync();
            }
        }
    }

    private void completeWaitingFutures(List<WaitingTask<T, R>> waitingNodes, CR multiResult, Throwable error) {
        if (error != null) {
            for (WaitingTask waitingNode : waitingNodes) {
                try {
                    waitingNode.future.completeExceptionally(error);
                } catch (Throwable t) {
                    waitingNode.future.completeExceptionally(t);
                }
            }
        } else {
            List<R> singleResults = combinedResultSplitter.apply(multiResult);
            for (int i = 0; i < waitingNodes.size(); i++) {
                try {
                    waitingNodes.get(i).future.complete(singleResults.get(i));
                } catch (Throwable t) {
                    waitingNodes.get(i).future.completeExceptionally(t);
                }
            }
        }
    }

    private R executeBatch(WaitingTask<T, R> currentWaitingNode) {
        List<WaitingTask<T, R>> waitingNodes = takeAllWaitingTasksOrFreeLock();

        if (waitingNodes.size() == 1) {
            T singleCommand = waitingNodes.get(0).wrappedTask;
            return taskExecutor.apply(singleCommand);
        }

        try {
            int resultIndex = -1;
            List<T> commandsInBatch = new ArrayList<>(waitingNodes.size());
            for (int i = 0; i < waitingNodes.size(); i++) {
                WaitingTask<T, R> waitingNode = waitingNodes.get(i);
                commandsInBatch.add(waitingNode.wrappedTask);
                if (waitingNode == currentWaitingNode) {
                    resultIndex = i;
                }
            }
            CT multiCommand = taskCombiner.apply(commandsInBatch);

            CR multiResult = combinedTaskExecutor.apply(multiCommand);
            List<R> singleResults = combinedResultSplitter.apply(multiResult);
            for (int i = 0; i < waitingNodes.size(); i++) {
                R singleResult = singleResults.get(i);
                waitingNodes.get(i).future.complete(singleResult);
            }

            return singleResults.get(resultIndex);
        } catch (Throwable e) {
            for (WaitingTask<T, R> waitingNode : waitingNodes) {
                waitingNode.future.completeExceptionally(e);
            }
            throw new BatchFailedException(e);
        }
    }

    private WaitingTask<T, R> lockExclusivelyOrEnqueue(T command) {
        WaitingTask<T, R> waitingTask = new WaitingTask<>(command);

        while (true) {
            WaitingTask<T, R> previous = headReference.get();
            if (previous == QUEUE_EMPTY) {
                if (headReference.compareAndSet(previous, QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS)) {
                    return null;
                } else {
                    continue;
                }
            }

            waitingTask.previous = previous;
            if (headReference.compareAndSet(previous, waitingTask)) {
                return waitingTask;
            } else {
                waitingTask.previous = null;
            }
        }
    }

    private void wakeupAnyThreadFromNextBatchOrFreeLock() {
        while (true) {
            WaitingTask previous = headReference.get();
            if (previous == QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS) {
                if (headReference.compareAndSet(QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS, QUEUE_EMPTY)) {
                    return;
                } else {
                    continue;
                }
            } else if (previous != QUEUE_EMPTY) {
                previous.future.complete(NEED_TO_EXECUTE_NEXT_BATCH);
                return;
            } else {
                // should never come there
                String msg = "Detected illegal usage of API, wakeupAnyThreadFromNextBatchOrFreeLock should not be called on empty queue";
                throw new IllegalStateException(msg);
            }
        }
    }

    private List<WaitingTask<T, R>> takeAllWaitingTasksOrFreeLock() {
        WaitingTask<T, R> head;
        while (true) {
            head = headReference.get();
            if (head == QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS) {
                if (headReference.compareAndSet(QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS, QUEUE_EMPTY)) {
                    return Collections.emptyList();
                } else {
                    continue;
                }
            }

            if (headReference.compareAndSet(head, QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS)) {
                break;
            }
        }

        WaitingTask<T, R> current = head;
        List<WaitingTask<T, R>> waitingNodes = new ArrayList<>();
        while (current != QUEUE_EMPTY_BUT_EXECUTION_IN_PROGRESS) {
            waitingNodes.add(current);
            WaitingTask<T, R> tmp = current.previous;
            current.previous = null; // nullify the reference to previous node in order to avoid GC nepotism
            current = tmp;
        }
        Collections.reverse(waitingNodes);
        return waitingNodes;
    }

    private static class WaitingTask<T, R> {

        public final T wrappedTask;
        public final CompletableFuture<R> future = new CompletableFuture<>();

        public WaitingTask<T, R> previous;

        WaitingTask(T task) {
            this.wrappedTask = task;
        }

        public R waitUninterruptedly() {
            boolean wasInterrupted = false;;
            try {
                while (true) {
                    wasInterrupted = wasInterrupted || Thread.interrupted();
                    try {
                        return future.get();
                    } catch (InterruptedException e) {
                        wasInterrupted = true;
                    } catch (ExecutionException e) {
                        throw new BatchFailedException(e.getCause());
                    }
                }
            } finally {
                if (wasInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static class BatchFailedException extends IllegalStateException {

        public BatchFailedException(Throwable e) {
            super(e);
        }

    }

}
