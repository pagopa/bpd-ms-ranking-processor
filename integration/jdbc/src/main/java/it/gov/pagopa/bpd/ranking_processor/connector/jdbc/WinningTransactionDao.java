package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Collection;
import java.util.List;

/**
 * Data Access Object to manage the operations to the database related to {@link WinningTransaction} model
 */
public interface WinningTransactionDao {

    /**
     * Sort to retrieve transactions sorted by fiscal code
     */
    Sort FIND_TRX_TO_PROCESS_PAGEABLE_SORT = Sort.by("fiscal_code_s");

    List<WinningTransaction> findPaymentToProcess(Long awardPeriodId, Pageable pageable);

    WinningTransaction findPaymentTrxWithCorrelationId(WinningTransaction.FilterCriteria filterCriteria);

    WinningTransaction findPaymentTrxWithoutCorrelationId(WinningTransaction.FilterCriteria filterCriteria);

    List<WinningTransaction> findTransferToProcess(WinningTransaction.FilterCriteria filterCriteria, Pageable pageable);

//    List<WinningTransaction> findTotalTransferToProcess(Long awardPeriodId, Pageable pageable);

    List<WinningTransaction> findPartialTranferToProcess(Long awardPeriodId, Pageable pageable);

    int[] updateProcessedTransaction(Collection<WinningTransaction> winningTransactionIds);

    int[] updateUnrelatedTransfer(Collection<WinningTransaction> winningTransactions);

    int[] updateUnprocessedPartialTransfer(Collection<WinningTransaction> winningTransactions);

    int[] deleteTransfer(List<WinningTransaction> winningTransactions);
}
