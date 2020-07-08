package org.apache.rocketmq.store.util;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 时间轮线程调度器
 *
 * @author zy-user
 */
public class TimeWheel {

    private ConcurrentLinkedDeque<Runnable>[] buckets;
    private volatile int currIndex = -1;
    private ExecutorService executorService;
    private long timeSpanInSeconds;
    private long startTime;
    private long scheduleCount;
    private AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * @param timeSpanInSeconds 时间轮的跨度
     * @param executorService   时间到了提交任务线程
     */
    public TimeWheel(int timeSpanInSeconds, ExecutorService executorService) {
        if (timeSpanInSeconds <= 0) {
            throw new IllegalArgumentException("wrong timeSpanInSeconds");
        }
        this.timeSpanInSeconds = timeSpanInSeconds;
        if (executorService == null || executorService.isShutdown()) {
            throw new IllegalArgumentException("wrong executorService");
        }
        this.executorService = executorService;
        this.buckets = new ConcurrentLinkedDeque[timeSpanInSeconds + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new ConcurrentLinkedDeque<>();
        }
    }

    public void addTask(long executeTimeInSeconds, Runnable runnable) {
        long now = System.currentTimeMillis() / 1000;
        if (executeTimeInSeconds <= now) {
            scheduledExecutorService.execute(runnable);
            return;
        }
        if (executeTimeInSeconds > now + timeSpanInSeconds) {
            throw new IllegalArgumentException("wrong executeTime");
        }
        int newIndex = (((int) (executeTimeInSeconds - now)) + currIndex) % buckets.length;
        buckets[newIndex].addFirst(runnable);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        startTime = System.currentTimeMillis() / 1000;
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                if (!started.get()) {
                    return;
                }
                // 本轮的index
                currIndex = (currIndex + 1) % buckets.length;
                ConcurrentLinkedDeque<Runnable> bucket = buckets[currIndex];
                Runnable r;
                while ((r = bucket.pollLast()) != null) {
                    executorService.execute(r);
                }
                // 调度的次数
                scheduleCount++;
                // 下一次执行的时间=调度的次数
                // 第一次调度在0s 第二次在1s ...
                // 这样来补偿因为线程延迟带来的误差
                long timeWait = scheduleCount - (System.currentTimeMillis() / 1000 - startTime);
                scheduledExecutorService.schedule(this, timeWait <= 0 ? 0 : timeWait,
                        TimeUnit.SECONDS);
            }
        }, 0, TimeUnit.SECONDS);
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            scheduledExecutorService.shutdown();
        }
    }

    public long getCurrentScheduleTime() {
        return startTime + scheduleCount;
    }

}
