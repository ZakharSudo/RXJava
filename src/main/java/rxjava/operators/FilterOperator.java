package rxjava.operators;

import rxjava.Observable;
import rxjava.Observer;
import java.util.function.Predicate;

public class FilterOperator<T> extends Observable<T> {
    
    public FilterOperator(Observable<T> source, Predicate<T> predicate) {
        super(observer -> {
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        if (predicate.test(item)) {
                            observer.onNext(item);
                        }
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