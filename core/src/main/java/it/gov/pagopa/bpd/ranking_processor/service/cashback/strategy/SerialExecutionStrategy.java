package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Serial implementation of {@link ExecutionStrategy}
 */
@Slf4j
@Service
class SerialExecutionStrategy implements ExecutionStrategy {

    @Override
    public <T, U> Map<T, U> unorderedMapSupplier() {
        return new HashMap<>();
    }

    @Override
    public <T> Stream<T> streamSupplier(Collection<T> collection) {
        return StreamSupport.stream(collection.spliterator(), false);
    }

    @Override
    public <T, K, U> Collector<T, ?, ? extends Map<K, U>> toMap(Function<? super T, ? extends K> keyMapper,
                                                                Function<? super T, ? extends U> valueMapper,
                                                                BinaryOperator<U> mergeFunction) {
        return Collectors.toMap(keyMapper, valueMapper, mergeFunction);
    }

}
