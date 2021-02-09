package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
class CashbackProcessingServiceImpl implements CashbackProcessingService {

    private final WinningTransactionDao winningTransactionDao;
    private final CitizenRankingDao citizenRankingDao;
    private final Integer limit;
    private final BinaryOperator<CitizenRanking> cashbackMapper;


    @Autowired
    public CashbackProcessingServiceImpl(WinningTransactionDao winningTransactionDao,
                                         CitizenRankingDao citizenRankingDao,
                                         @Value("${ranking-processor-cashback-processing.data-extraction.limit}") Integer limit) {
        this.winningTransactionDao = winningTransactionDao;
        this.citizenRankingDao = citizenRankingDao;
        this.limit = limit;
        cashbackMapper = (cr1, cr2) -> {
            cr1.setTotalCashback(cr1.getTotalCashback().add(cr2.getTotalCashback()));
            cr1.setTransactionNumber(cr1.getTransactionNumber() + cr2.getTransactionNumber());
            return cr1;
        };
    }

    public void processCashback(final Long awardPeriodId) {
        for (TransactionType trxType : TransactionType.values()) {
            int pageNumber = 0;
            int trxCount;
            do {
                trxCount = processCashback(awardPeriodId, trxType, PageRequest.of(pageNumber++, limit));
            } while (limit == trxCount);
        }
    }

    @Override
    @Transactional
    public int processCashback(final Long awardPeriodId, final TransactionType transactionType, Pageable pageable) {
        List<WinningTransaction> transactions = winningTransactionDao.findTransactionToProcess(awardPeriodId,
                transactionType,
                pageable);

        ConcurrentMap<String, CitizenRanking> cashbackMap = aggregateData(awardPeriodId, transactions);

        int[] affectedRows = citizenRankingDao.updateCashback(cashbackMap.values());
        if (affectedRows.length != cashbackMap.values().size()) {
            log.warn("updateCashback: affected {} rows of {}", affectedRows.length, cashbackMap.values().size());
        }

        affectedRows = winningTransactionDao.updateProcessedTransaction(transactions);
        if (affectedRows.length != cashbackMap.values().size()) {
            log.warn("updateProcessedTransaction: affected {} rows of {}", affectedRows.length, cashbackMap.values().size());
        }

        return transactions.size();
    }


    private ConcurrentMap<String, CitizenRanking> aggregateData(final Long awardPeriodId, final List<WinningTransaction> transactions) {
        return transactions.stream()
                .parallel()
                .map(trx -> CitizenRanking.builder()
                        .fiscalCode(trx.getFiscalCode())
                        .awardPeriodId(awardPeriodId)
                        .totalCashback(trx.getScore())
                        .transactionNumber("01".equals(trx.getOperationType()) ? -1L : 1L)
                        .build())
                .collect(Collectors.toConcurrentMap(CitizenRanking::getFiscalCode, Function.identity(), cashbackMapper));
    }


}
