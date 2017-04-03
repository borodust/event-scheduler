import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.time.LocalDateTime.now;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Thread-safe scheduler for dispatching tasks at specific time.
 *
 * @author Pavel Korolev <dev at borodust.org>
 */
public class TaskScheduler implements AutoCloseable {
    private final ScheduledExecutorService schedulingExecutor =
            Executors.newSingleThreadScheduledExecutor();
    private final TreeMap<LocalDateTime, List<Callable<?>>> queue =
            new TreeMap<>();

    private static void call (Callable<?> callable) {
        try {
            callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void process () {
        final List<Callable<?>> callables;
        synchronized (queue) {
            callables = queue.pollFirstEntry().getValue();
        }
        callables.forEach(TaskScheduler::call);
    }

    /**
     * Executes provided callable at specified time. Runs tasks in submission order,
     * if their scheduled execution time is the same. If provided time is less than
     * current time, schedules immediate execution respecting time of tasks already
     * in the queue, i.e. runs it in chronological order with other tasks in the queue.
     * Thread-safe.
     *
     * @param time task execution time
     * @param callable task to be executed at provided time
     */
    public void schedule(LocalDateTime time, Callable<?> callable) {
        final boolean schedulingRequired;

        synchronized (queue) {
            List<Callable<?>> simultaneousTaskQueue =
                    queue.computeIfAbsent(time, k -> new ArrayList<>());
            schedulingRequired = simultaneousTaskQueue.isEmpty();
            simultaneousTaskQueue.add(callable);
        }

        if (schedulingRequired) {
            long delay = Duration.between(now(), time).toMillis();
            schedulingExecutor.schedule(this::process, delay, MILLISECONDS);
        }
    }

    @Override
    public void close() {
        schedulingExecutor.shutdown();
    }
}
