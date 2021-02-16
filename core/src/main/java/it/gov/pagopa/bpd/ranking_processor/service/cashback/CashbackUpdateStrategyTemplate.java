package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Template Method pattern of {@link CashbackUpdateStrategy}
 */
@Slf4j
abstract class CashbackUpdateStrategyTemplate implements CashbackUpdateStrategy {

    private final WinningTransactionDao winningTransactionDao;
    private final CitizenRankingDao citizenRankingDao;
    private final BinaryOperator<CitizenRanking> cashbackMapper = (cr1, cr2) -> {
        cr1.setTotalCashback(cr1.getTotalCashback().add(cr2.getTotalCashback()));
        cr1.setTransactionNumber(cr1.getTransactionNumber() + cr2.getTransactionNumber());
        return cr1;
    };


    public CashbackUpdateStrategyTemplate(WinningTransactionDao winningTransactionDao,
                                          CitizenRankingDao citizenRankingDao) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyTemplate.CashbackUpdateStrategyTemplate");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionDao = {}, citizenRankingDao = {}", winningTransactionDao, citizenRankingDao);
        }

        this.winningTransactionDao = winningTransactionDao;
        this.citizenRankingDao = citizenRankingDao;
    }


    @Override
    @Transactional
    public int process(long awardPeriodId, TransactionType transactionType, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyTemplate.process");
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
            log.trace("CashbackUpdateStrategyTemplate.aggregateData");
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

        return aggregateData(stream, CitizenRanking::getFiscalCode, Function.identity(), cashbackMapper);
    }


    protected abstract Map<String, CitizenRanking> aggregateData(Stream<CitizenRanking> stream,
                                                                 Function<CitizenRanking, String> keyMapper,
                                                                 Function<CitizenRanking, CitizenRanking> valueMapper,
                                                                 BinaryOperator<CitizenRanking> mergeFunction);

}
