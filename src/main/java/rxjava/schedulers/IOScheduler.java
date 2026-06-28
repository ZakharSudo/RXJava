package rxjava.schedulers;

import rxjava.Scheduler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IOScheduler implements Scheduler {
    
    private final ExecutorService executor;
    
    public IOScheduler() {
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "RxIO-" + System.currentTimeMillis());
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
        return "IO-Scheduler";
    }
    
    public void shutdown() {
        executor.shutdown();
    }
}