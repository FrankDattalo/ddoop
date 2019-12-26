package ddoop.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadPool wraps executor services making it easier
 * to construct threads to run as daemons or in the 
 * foreground.
 */
public class ThreadPool {

    /**
     * A modified Runnable interface which can be 
     * interrupted.
     */
    public static interface InterruptableRunnable {
        public void run() throws InterruptedException;
    }

    /**
     * A handle to a currently executing task within
     * one of the tread pools.
     */
    public static class Task {

        private final Future<?> future;

        private Task(Future<?> future) {
            this.future = future;
        }

        /**
         * Cancels execution of the task if it is not already complete.
         */
        public void cancel() {
            this.future.cancel(true);
        }
    }

    private static ThreadFactory threadFactory(boolean daemon) {
        return new ThreadFactory() {
        
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    private final ExecutorService daemonThreadPool = Executors.newCachedThreadPool(threadFactory(true));
    
    private final ExecutorService foregroundThreadPool = Executors.newCachedThreadPool(threadFactory(true));

    /**
     * Runs the task on a daemon thread.
     * @param toRun the task to run.
     * @return a handle to that running task.
     */
    public Task onDaemonThread(InterruptableRunnable toRun) {
        return submit(this.daemonThreadPool, toRun);
    }

    /**
     * Runs the task on a foreground thread.
     * @param toRun the task to run.
     * @return a handle to that running task.
     */
    public Task onForegroundThread(InterruptableRunnable toRun) {
        return submit(this.foregroundThreadPool, toRun);
    }

    private Task submit(ExecutorService executorService, InterruptableRunnable toRun) {
        return new Task(executorService.submit(() -> {
            toRun.run();
            return null;
        }));
    }

}