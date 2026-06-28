# RxJava Custom Library

##  Описание проекта

Данный проект представляет собой реализацию упрощенной версии реактивной библиотеки RxJava, разработанную в рамках учебного задания. Библиотека реализует основные концепции реактивного программирования: паттерн «Наблюдатель» (Observer), асинхронное выполнение, обработку ошибок и операторы преобразования данных.

### Цель работы
Создать систему реактивных потоков с возможностью управления потоками выполнения (Schedulers) и обработки событий с использованием паттерна «Наблюдатель» (Observer pattern).

### Технологии
- Java 8+
- Потоки выполнения (java.util.concurrent)
- Паттерн проектирования «Наблюдатель»

### Компоненты системы

#### 1. Observer<T>
Интерфейс наблюдателя, получающий уведомления от Observable.

**Методы:**
- `void onNext(T item)` - вызывается при получении нового элемента
- `void onError(Throwable throwable)` - вызывается при возникновении ошибки
- `void onComplete()` - вызывается при успешном завершении потока

#### 2. Observable<T>
Основной класс, представляющий поток данных.

**Методы создания:**
- `just(T... items)` - создает поток из переданных элементов
- `fromIterable(Iterable<T> iterable)` - создает поток из коллекции
- `create(OnSubscribe<T> onSubscribe)` - создает поток с кастомной логикой

**Методы подписки:**
- `subscribe(Observer<T> observer)` - подписывает наблюдателя
- `subscribe(Consumer<T>, Consumer<Throwable>, Runnable)` - упрощенная подписка

**Методы управления потоками:**
- `subscribeOn(Scheduler scheduler)` - задает Scheduler для подписки
- `observeOn(Scheduler scheduler)` - задает Scheduler для обработки

#### 3. Scheduler
Интерфейс для управления потоками выполнения.

**Реализации:**
- **IOScheduler** - использует CachedThreadPool для IO-операций
- **ComputationScheduler** - использует FixedThreadPool (число ядер) для вычислений
- **SingleScheduler** - использует SingleThreadExecutor для последовательных операций

#### 4. Disposable
Интерфейс для управления подпиской.

**Методы:**
- `void dispose()` - отменяет подписку
- `boolean isDisposed()` - проверяет, отменена ли подписка

#### 5. Operators
**MapOperator<T,R>**
```java
Observable.just(1, 2, 3)
    .map(item -> item * 2)  // Преобразует: 1→2, 2→4, 3→6
    .subscribe(item -> System.out.println(item));

FilterOperator<T>

java
Observable.just(1, 2, 3, 4, 5, 6)
    .filter(item -> item % 2 == 0)  // Оставляет: 2, 4, 6
    .subscribe(item -> System.out.println(item));
FlatMapOperator<T,R>

java
Observable.just("1,2,3", "4,5,6")
    .flatMap(str -> {
        String[] parts = str.split(",");
        Integer[] numbers = Arrays.stream(parts)
            .map(Integer::parseInt)
            .toArray(Integer[]::new);
        return Observable.just(numbers);
    })
    .subscribe(item -> System.out.println(item));
    // Результат: 1, 2, 3, 4, 5, 6

```
### Принципы работы Schedulers
1. IOScheduler
Назначение: Для операций ввода-вывода (сеть, файловая система)

Особенности:

Использует Executors.newCachedThreadPool()

Создает новые потоки при необходимости

Переиспользует существующие потоки

Потоки завершаются после 60 секунд простоя

Пример использования:

java
IOScheduler io = new IOScheduler();
Observable.just("file1.txt", "file2.txt")
    .subscribeOn(io)
    .map(filename -> readFile(filename))
    .subscribe(content -> processFile(content));
2. ComputationScheduler
Назначение: Для вычислительных операций (обработка данных, трансформации)

Особенности:

Использует Executors.newFixedThreadPool(N_CPU)

Количество потоков = число ядер процессора

Оптимален для CPU-bound задач

Потоки не завершаются до shutdown

Пример использования:

java
ComputationScheduler comp = new ComputationScheduler();
Observable.just(1, 2, 3, 4, 5)
    .observeOn(comp)
    .map(item -> expensiveCalculation(item))
    .subscribe(result -> displayResult(result));
3. SingleScheduler
Назначение: Для последовательных операций, требующих порядка выполнения

Особенности:

Использует Executors.newSingleThreadExecutor()
Один поток для всех задач
Гарантирует порядок выполнения
Все задачи выполняются последовательно

Пример использования:

java
SingleScheduler single = new SingleScheduler();
Observable.just(event1, event2, event3)
    .subscribeOn(single)
    .subscribe(event -> processEvent(event));

Методы управления потоками
subscribeOn()
Определяет Scheduler для выполнения подписки (источника данных):

java
observable.subscribeOn(new IOScheduler())
Влияет на выполнение всей цепочки "вверх" от оператора

Определяет поток для создания данных

observeOn()
Определяет Scheduler для обработки элементов:

java
observable.observeOn(new ComputationScheduler())
Влияет на выполнение всей цепочки "вниз" от оператора

Определяет поток для обработки данных

📊 Демонстрация работы
Запуск демонстрации
bash
# Компиляция
javac -d . src/main/java/rxjava/*.java src/main/java/rxjava/schedulers/*.java src/main/java/rxjava/operators/*.java src/main/java/Main.java

# Запуск
java -cp . Main
Результаты выполнения
1. Базовые операторы
text
Исходный поток: 1 2 3 4 5
map: 2 4 6 8 10
filter: 2 4 6 8 10
2. Обработка ошибок
text
Получено: 1
Получено: 2
Генерируем ошибку...
Ошибка обработана: Тестовая ошибка
3. Schedulers
text
IO Scheduler: выполняет в потоках RxIO-*
Computation Scheduler: выполняет в потоках RxComputation-*
Single Scheduler: выполняет в одном потоке RxSingle-*
4. flatMap
text
"1,2,3", "4,5,6", "7,8,9" → 1 2 3 4 5 6 7 8 9
5. Disposable
text
Создан долгий поток, отменен через 2 секунды
6. Сложная цепочка
text
1-15 → filter >5 → map *2 → filter <20 → Результат: 12, 14, 16, 18

<pre style="font-family: inherit;">   
Жизненный цикл потока
text
1. Создание Observable
   ↓
2. Подписка (subscribe)
   ↓
3. Выполнение onSubscribe (в потоке subscribeOn)
   ↓
4. Генерация элементов
   ↓
5. Применение операторов (в потоке observeOn)
   ↓
6. Доставка элементов Observer (onNext)
   ↓
7. Завершение (onComplete) или Ошибка (onError)
   ↓
8. Отмена подписки (dispose)
</pre>
