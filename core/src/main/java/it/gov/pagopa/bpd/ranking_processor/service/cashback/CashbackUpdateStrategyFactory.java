package it.gov.pagopa.bpd.ranking_processor.service.cashback;

/**
 * Abstract Factory of {@link CashbackUpdateStrategy}
 */
public interface CashbackUpdateStrategyFactory {

    CashbackUpdateStrategy create();

}
