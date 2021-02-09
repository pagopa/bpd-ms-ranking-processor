package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.springframework.data.domain.Pageable;

public interface CashbackProcessingService {

    int processCashback(Long awardPeriodId, WinningTransaction.TransactionType transactionType, Pageable pageable);

}
