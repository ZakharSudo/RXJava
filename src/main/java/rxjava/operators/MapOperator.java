package rxjava.operators;

import rxjava.Observable;
import rxjava.Observer;
import java.util.function.Function;

public class MapOperator<T, R> extends Observable<R> {
    
    public MapOperator(Observable<T> source, Function<T, R> mapper) {
        super(observer -> {
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        R result = mapper.apply(item);
                        observer.onNext(result);
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
                    observer.onComplete();
                }
            });
        });
    }
}