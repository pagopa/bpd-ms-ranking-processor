package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.springframework.data.domain.Pageable;

import java.util.Collection;
import java.util.List;

/**
 * Data Access Object to manage the operations to the database related to {@link WinningTransaction} model
 */
public interface WinningTransactionDao {

    List<WinningTransaction> findTransactionToProcess(Long awardPeriodId,
                                                      WinningTransaction.TransactionType transactionType,
                                                      Pageable pageable);

    List<WinningTransaction> findPaymentToProcess(Long awardPeriodId, Pageable pageable);

    List<WinningTransaction> findTotalTransferToProcess(Long awardPeriodId, Pageable pageable);

    List<WinningTransaction> findPartialTranferToProcess(Long awardPeriodId, Pageable pageable);

    int[] updateProcessedTransaction(Collection<WinningTransaction> winningTransactionIds);

}
