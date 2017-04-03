import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.time.LocalDateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Pavel Korolev <dev at borodust.org>
 */
public class TaskSchedulerTest {
    private final ExecutorService producer = Executors.newFixedThreadPool(4);

    private LocalDateTime plusMillis(int millis) {
        return now().plus(millis, ChronoUnit.MILLIS);
    }

    /**
     * Doesn't cover all possible input or states. More like sanity test.
     */
    @Test
    public void testScheduling() throws InterruptedException {
        List<LocalDateTime> timings = Collections.synchronizedList(Arrays.asList(
                plusMillis(100),
                plusMillis(300),
                plusMillis(100),
                plusMillis(300),
                plusMillis(200),
                plusMillis(0),
                plusMillis(0),
                plusMillis(0)));

        CountDownLatch latch = new CountDownLatch(timings.size());
        List<LocalDateTime> result = Collections.synchronizedList(new ArrayList<>());

        try (final TaskScheduler scheduler = new TaskScheduler()) {
            timings.forEach(time ->
                    producer.execute(() ->
                            scheduler.schedule(time, () -> {
                                result.add(time);
                                latch.countDown();
                                return null;
                            })));
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        }
        Collections.sort(timings);
        assertEquals(result, timings);
    }
}
