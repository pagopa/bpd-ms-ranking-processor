package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;

/**
 * A service to manage the Business Logic related to cashback update process
 */
public interface CashbackUpdateService {

    int process(final long awardPeriodId, WinningTransaction.TransactionType transactionType, SimplePageRequest simplePageRequest);

}
