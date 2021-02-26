package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

/**
 * Abstract Factory of {@link ExecutionStrategy}
 */
public interface ExecutionStrategyFactory {

    ExecutionStrategy create();

}
