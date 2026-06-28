package rxjava;

public interface Scheduler {
    void execute(Runnable task);
    default String getName() {
        return getClass().getSimpleName();
    }
}