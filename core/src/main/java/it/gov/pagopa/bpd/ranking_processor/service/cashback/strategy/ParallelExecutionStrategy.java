package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.*;

/**
 * Parallel implementation of {@link ExecutionStrategy}
 */
@Slf4j
@Service
class ParallelExecutionStrategy implements ExecutionStrategy {

    @Override
    public <T, U> Map<T, U> unorderedMapSupplier() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public <T> Stream<T> streamSupplier(Collection<T> collection) {
        return StreamSupport.stream(collection.spliterator(), true);
    }

    @Override
    public IntStream intStreamSupplier(Spliterator.OfInt ofInt) {
        return StreamSupport.intStream(ofInt, false);
    }

    @Override
    public <T, K, U> Collector<T, ?, ? extends Map<K, U>> toMap(Function<? super T, ? extends K> keyMapper,
                                                                Function<? super T, ? extends U> valueMapper,
                                                                BinaryOperator<U> mergeFunction) {
        return Collectors.toConcurrentMap(keyMapper, valueMapper, mergeFunction);
    }

}
