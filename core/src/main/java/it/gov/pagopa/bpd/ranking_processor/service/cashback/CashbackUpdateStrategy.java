package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;

/**
 * A Strategy Pattern to manage the Business Logic related to cashback update process
 */
public interface CashbackUpdateStrategy {

    int process(final long awardPeriodId, WinningTransaction.TransactionType transactionType, SimplePageRequest simplePageRequest);

}
