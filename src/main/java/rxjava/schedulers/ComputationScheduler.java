package rxjava.schedulers;

import rxjava.Scheduler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ComputationScheduler implements Scheduler {
    
    private final ExecutorService executor;
    
    public ComputationScheduler() {
        int processors = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(
            processors,
            r -> {
                Thread thread = new Thread(r, "RxComputation-" + System.currentTimeMillis());
                thread.setDaemon(true);
                return thread;
            }
        );
    }
    
    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
    
    @Override
    public String getName() {
        return "Computation-Scheduler";
    }
    
    public void shutdown() {
        executor.shutdown();
    }
}