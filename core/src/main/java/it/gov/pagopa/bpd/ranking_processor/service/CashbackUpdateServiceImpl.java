package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * See {@link CashbackUpdateService}
 */
@Slf4j
@Service
class CashbackUpdateServiceImpl implements CashbackUpdateService {

    private final WinningTransactionDao winningTransactionDao;
    private final CitizenRankingDao citizenRankingDao;
    private final boolean parallelEnabled;
    private final BinaryOperator<CitizenRanking> cashbackMapper = (cr1, cr2) -> {
        cr1.setTotalCashback(cr1.getTotalCashback().add(cr2.getTotalCashback()));
        cr1.setTransactionNumber(cr1.getTransactionNumber() + cr2.getTransactionNumber());
        return cr1;
    };


    @Autowired
    public CashbackUpdateServiceImpl(WinningTransactionDao winningTransactionDao,
                                     CitizenRankingDao citizenRankingDao,
                                     @Value("${cashback-update.parallel.enable}") boolean parallelEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateServiceImpl.CashbackUpdateServiceImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionDao = {}, citizenRankingDao = {}, parallelEnabled = {}", winningTransactionDao, citizenRankingDao, parallelEnabled);
        }

        this.winningTransactionDao = winningTransactionDao;
        this.citizenRankingDao = citizenRankingDao;
        this.parallelEnabled = parallelEnabled;
    }


    @Override
    @Transactional
    public int process(long awardPeriodId, TransactionType transactionType, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateServiceImpl.processCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, transactionType = {}, simplePageRequest = {}", awardPeriodId, transactionType, simplePageRequest);
        }
        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(), simplePageRequest.getSize());

        List<WinningTransaction> transactions = winningTransactionDao.findTransactionToProcess(awardPeriodId,
                transactionType,
                pageRequest);

        Map<String, CitizenRanking> cashbackMap = aggregateData(awardPeriodId, transactions);

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

    private Map<String, CitizenRanking> aggregateData(final Long awardPeriodId, final List<WinningTransaction> transactions) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateServiceImpl.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, transactions = {}", awardPeriodId, transactions);
        }

        Stream<CitizenRanking> stream = transactions.stream()
                .map(trx -> CitizenRanking.builder()
                        .fiscalCode(trx.getFiscalCode())
                        .awardPeriodId(awardPeriodId)
                        .totalCashback(trx.getScore())
                        .transactionNumber("01".equals(trx.getOperationType()) ? -1L : 1L)
                        .build());
        return parallelEnabled
                ? stream.parallel().collect(Collectors.toConcurrentMap(CitizenRanking::getFiscalCode, Function.identity(), cashbackMapper))
                : stream.collect(Collectors.toMap(CitizenRanking::getFiscalCode, Function.identity(), cashbackMapper));
    }

}
