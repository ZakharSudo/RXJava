package rxjava;

import rxjava.operators.*;
import rxjava.schedulers.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Observable<T> {
    
    private final OnSubscribe<T> onSubscribe;
    
    @FunctionalInterface
    public interface OnSubscribe<T> {
        void subscribe(Observer<T> observer);
    }
    
    // Изменено с private на protected
    protected Observable(OnSubscribe<T> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }
    
    @SafeVarargs
    public static <T> Observable<T> just(T... items) {
        return new Observable<>(observer -> {
            try {
                for (T item : items) {
                    if (observer instanceof SubscriptionObserver) {
                        SubscriptionObserver<?> subObs = (SubscriptionObserver<?>) observer;
                        if (subObs.isDisposed()) {
                            break;
                        }
                    }
                    observer.onNext(item);
                }
                observer.onComplete();
            } catch (Exception e) {
                observer.onError(e);
            }
        });
    }
    
    public static <T> Observable<T> fromIterable(Iterable<T> iterable) {
        return new Observable<>(observer -> {
            try {
                for (T item : iterable) {
                    if (observer instanceof SubscriptionObserver) {
                        SubscriptionObserver<?> subObs = (SubscriptionObserver<?>) observer;
                        if (subObs.isDisposed()) {
                            break;
                        }
                    }
                    observer.onNext(item);
                }
                observer.onComplete();
            } catch (Exception e) {
                observer.onError(e);
            }
        });
    }
    
    public static <T> Observable<T> create(OnSubscribe<T> onSubscribe) {
        return new Observable<>(onSubscribe);
    }
    
    public Disposable subscribe(Observer<T> observer) {
        SubscriptionObserver<T> subscriptionObserver = new SubscriptionObserver<>(observer);
        try {
            onSubscribe.subscribe(subscriptionObserver);
        } catch (Exception e) {
            observer.onError(e);
        }
        return subscriptionObserver;
    }
    
    public Disposable subscribe(
            java.util.function.Consumer<T> onNext,
            java.util.function.Consumer<Throwable> onError,
            Runnable onComplete) {
        
        Observer<T> observer = new Observer<T>() {
            @Override
            public void onNext(T item) {
                onNext.accept(item);
            }
            
            @Override
            public void onError(Throwable throwable) {
                onError.accept(throwable);
            }
            
            @Override
            public void onComplete() {
                onComplete.run();
            }
        };
        
        return subscribe(observer);
    }
    
    public <R> Observable<R> map(java.util.function.Function<T, R> mapper) {
        return new MapOperator<>(this, mapper);
    }
    
    public Observable<T> filter(java.util.function.Predicate<T> predicate) {
        return new FilterOperator<>(this, predicate);
    }
    
    public <R> Observable<R> flatMap(java.util.function.Function<T, Observable<R>> mapper) {
        return new FlatMapOperator<>(this, mapper);
    }
    
    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(observer -> {
            scheduler.execute(() -> {
                onSubscribe.subscribe(observer);
            });
        });
    }
    
    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(observer -> {
            onSubscribe.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    scheduler.execute(() -> observer.onNext(item));
                }
                
                @Override
                public void onError(Throwable throwable) {
                    scheduler.execute(() -> observer.onError(throwable));
                }
                
                @Override
                public void onComplete() {
                    scheduler.execute(() -> observer.onComplete());
                }
            });
        });
    }
    
    private static class SubscriptionObserver<T> implements Observer<T>, Disposable {
        private final Observer<T> actual;
        private final AtomicBoolean disposed = new AtomicBoolean(false);
        
        SubscriptionObserver(Observer<T> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onNext(T item) {
            if (!isDisposed()) {
                actual.onNext(item);
            }
        }
        
        @Override
        public void onError(Throwable throwable) {
            if (!isDisposed()) {
                actual.onError(throwable);
                dispose();
            }
        }
        
        @Override
        public void onComplete() {
            if (!isDisposed()) {
                actual.onComplete();
                dispose();
            }
        }
        
        @Override
        public void dispose() {
            disposed.set(true);
        }
        
        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}