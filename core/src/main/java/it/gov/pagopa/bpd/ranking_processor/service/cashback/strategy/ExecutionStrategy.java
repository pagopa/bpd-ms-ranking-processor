package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A Strategy Pattern to manage the execution mode related to cashback update process
 */
public interface ExecutionStrategy {

    <T, U> Map<T, U> unorderedMapSupplier();

    <T> Stream<T> streamSupplier(Collection<T> collection);

    <T, K, U> Collector<T, ?, ? extends Map<K, U>> toMap(Function<? super T, ? extends K> keyMapper,
                                                         Function<? super T, ? extends U> valueMapper,
                                                         BinaryOperator<U> mergeFunction);

}
