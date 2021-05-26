package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.ActiveUserWinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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

    List<ActiveUserWinningTransaction> findActiveUsersSinceLastDetector(Long awardPeriodId);

    List<WinningTransaction> findTopValidWinningTransactions(Long awardPeriodId, int validPayments, String fiscalCode, String merchant, LocalDate paymentDate);

    int updateDetectorLastExecution(OffsetDateTime maxInsertDate);

    int updateInvalidateTransactions(String fiscalCode, LocalDate paymentDate, String merchant, Long awardPeriodId);

    int[] updateSetValidTransactions(List<WinningTransaction> winningTransactionList);

    int updateUserTransactionsElab(String fiscalCode, Long awardPeriodId);
}
