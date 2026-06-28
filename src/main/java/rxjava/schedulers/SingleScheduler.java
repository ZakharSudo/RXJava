package rxjava.schedulers;

import rxjava.Scheduler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SingleScheduler implements Scheduler {
    
    private final ExecutorService executor;
    
    public SingleScheduler() {
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "RxSingle-" + System.currentTimeMillis());
            thread.setDaemon(true);
            return thread;
        });
    }
    
    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
    
    @Override
    public String getName() {
        return "Single-Scheduler";
    }
    
    public void shutdown() {
        executor.shutdown();
    }
}