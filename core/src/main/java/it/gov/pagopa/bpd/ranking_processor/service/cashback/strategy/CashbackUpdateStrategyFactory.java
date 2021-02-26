package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;

/**
 * Abstract Factory of {@link CashbackUpdateStrategy}
 */
public interface CashbackUpdateStrategyFactory {

    CashbackUpdateStrategy create(WinningTransaction.TransactionType trxType);

}
