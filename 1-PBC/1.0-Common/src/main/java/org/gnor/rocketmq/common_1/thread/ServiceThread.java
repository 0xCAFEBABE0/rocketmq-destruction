package org.gnor.rocketmq.common_1.thread;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ServiceThread implements Runnable{
    private static final long JOIN_TIME = 90 * 1000;

    protected Thread thread;
    protected volatile boolean stopped = false;
    protected boolean isDaemon = false;
    private final AtomicBoolean started = new AtomicBoolean(false);

    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    public abstract String getServiceName();

    public void start() {
        System.out.println("Try to start service " + getServiceName());
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
        System.out.println("Service " + getServiceName() + " started");
    }

    public void shutdown() {
        System.out.println("Try to shutdown service " + getServiceName());
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        try {

            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Service " + getServiceName() + " stopped");
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        System.out.println("makeStop " + getServiceName());
    }

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown();
        }
    }

    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        waitPoint.reset();
        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }
    protected void onWaitEnd() {
    }


}
