package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;

public interface CashbackUpdateService {

    int processCashback(Long awardPeriodId, WinningTransaction.TransactionType transactionType, int pageNumber);

}
