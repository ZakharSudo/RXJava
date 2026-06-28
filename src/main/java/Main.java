import rxjava.*;
import rxjava.schedulers.*;

public class Main {
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Демонстрация RxJava библиотеки ===\n");
        
        demoBasicOperators();
        Thread.sleep(1000);
        
        demoErrorHandling();
        Thread.sleep(1000);
        
        demoSchedulers();
        Thread.sleep(1000);
        
        demoFlatMap();
        Thread.sleep(1000);
        
        demoDisposable();
        Thread.sleep(1000);
        
        demoComplexChain();
    }
    
    private static void demoBasicOperators() {
        System.out.println("--- 1. Базовые операторы ---");
        
        System.out.println("Исходный поток:");
        Observable.just(1, 2, 3, 4, 5)
            .subscribe(
                item -> System.out.print(item + " "),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("\nПоток завершен\n")
            );
        
        System.out.println("map: умножение на 2");
        Observable.just(1, 2, 3, 4, 5)
            .map(item -> item * 2)
            .subscribe(
                item -> System.out.print(item + " "),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("\nПоток завершен\n")
            );
        
        System.out.println("filter: только четные числа");
        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .filter(item -> item % 2 == 0)
            .subscribe(
                item -> System.out.print(item + " "),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("\nПоток завершен\n")
            );
    }
    
    private static void demoErrorHandling() {
        System.out.println("--- 2. Обработка ошибок ---");
        
        Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            System.out.println("Генерируем ошибку...");
            throw new RuntimeException("Тестовая ошибка");
        })
        .subscribe(
            item -> System.out.println("Получено: " + item),
            error -> System.err.println("Ошибка обработана: " + error.getMessage()),
            () -> System.out.println("Поток завершен\n")
        );
    }
    
    private static void demoSchedulers() throws InterruptedException {
        System.out.println("--- 3. Schedulers ---");
        
        System.out.println("IO Scheduler:");
        IOScheduler io = new IOScheduler();
        Observable.just(1, 2, 3)
            .subscribeOn(io)
            .map(item -> {
                System.out.println("  " + Thread.currentThread().getName());
                return item * 2;
            })
            .subscribe(
                item -> System.out.println("  Результат: " + item),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("  IO завершен\n")
            );
        
        Thread.sleep(500);
        
        System.out.println("Computation Scheduler:");
        ComputationScheduler comp = new ComputationScheduler();
        Observable.just(1, 2, 3)
            .observeOn(comp)
            .map(item -> {
                System.out.println("  " + Thread.currentThread().getName());
                return item * item;
            })
            .subscribe(
                item -> System.out.println("  Результат: " + item),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("  Computation завершен\n")
            );
        
        Thread.sleep(500);
        
        System.out.println("Single Scheduler:");
        SingleScheduler single = new SingleScheduler();
        Observable.just(1, 2, 3)
            .subscribeOn(single)
            .subscribe(
                item -> System.out.println("  " + Thread.currentThread().getName() + ": " + item),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("  Single завершен\n")
            );
        
        Thread.sleep(500);
    }
    
    private static void demoFlatMap() {
        System.out.println("--- 4. flatMap ---");
        
        System.out.println("Разбиение строк на числа:");
        Observable.just("1,2,3", "4,5,6", "7,8,9")
            .flatMap(str -> {
                String[] parts = str.split(",");
                Integer[] numbers = java.util.Arrays.stream(parts)
                    .map(Integer::parseInt)
                    .toArray(Integer[]::new);
                return Observable.just(numbers);
            })
            .subscribe(
                item -> System.out.print(item + " "),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("\nflatMap завершен\n")
            );
    }
    
    private static void demoDisposable() {
        System.out.println("--- 5. Disposable ---");
        
        System.out.println("Создаем долгий поток...");
        Disposable disposable = Observable.create(observer -> {
            try {
                for (int i = 1; i <= 10; i++) {
                    if (Thread.currentThread().isInterrupted()) {
                        observer.onComplete();
                        return;
                    }
                    observer.onNext(i);
                    Thread.sleep(500);
                }
                observer.onComplete();
            } catch (InterruptedException e) {
                observer.onError(e);
            }
        })
        .subscribe(
            item -> System.out.println("  Получено: " + item),
            error -> System.err.println("Error: " + error),
            () -> System.out.println("  Поток завершен")
        );
        
        System.out.println("Отменяем подписку через 2 секунды...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        disposable.dispose();
        System.out.println("Подписка отменена: " + disposable.isDisposed() + "\n");
    }
    
    private static void demoComplexChain() {
        System.out.println("--- 6. Сложная цепочка операторов ---");
        
        System.out.println("Обработка списка чисел:");
        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
            .filter(item -> item > 5)
            .map(item -> item * 2)
            .filter(item -> item < 20)
            .map(item -> "Результат: " + item)
            .subscribe(
                item -> System.out.println("  " + item),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("  Цепочка завершена\n")
            );
    }
}