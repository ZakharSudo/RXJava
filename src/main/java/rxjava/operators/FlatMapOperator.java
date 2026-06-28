package rxjava.operators;

import rxjava.Observable;
import rxjava.Observer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatMapOperator<T, R> extends Observable<R> {
    
    public FlatMapOperator(Observable<T> source, java.util.function.Function<T, Observable<R>> mapper) {
        super(observer -> {
            List<R> results = new ArrayList<>();
            AtomicInteger pending = new AtomicInteger(1);
            
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        Observable<R> observable = mapper.apply(item);
                        pending.incrementAndGet();
                        
                        observable.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R result) {
                                observer.onNext(result);
                            }
                            
                            @Override
                            public void onError(Throwable throwable) {
                                observer.onError(throwable);
                            }
                            
                            @Override
                            public void onComplete() {
                                if (pending.decrementAndGet() == 0) {
                                    observer.onComplete();
                                }
                            }
                        });
                    } catch (Exception e) {
                        observer.onError(e);
                    }
                }
                
                @Override
                public void onError(Throwable throwable) {
                    observer.onError(throwable);
                }
                
                @Override
                public void onComplete() {
                    if (pending.decrementAndGet() == 0) {
                        observer.onComplete();
                    }
                }
            });
        });
    }
}