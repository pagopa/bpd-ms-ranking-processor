package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;

import java.util.List;

public interface WinningTransactionDao {

    List<WinningTransaction> findTransactionToProcess(Long awardPeriodId, WinningTransaction.TransactionType transactionType);

}
