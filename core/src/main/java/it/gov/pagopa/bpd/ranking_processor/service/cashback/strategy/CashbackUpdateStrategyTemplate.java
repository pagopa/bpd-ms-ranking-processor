package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.CashbackUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Template Method pattern of {@link CashbackUpdateStrategy}
 */
@Slf4j
abstract class CashbackUpdateStrategyTemplate implements CashbackUpdateStrategy {

    static final String ERROR_MESSAGE_TEMPLATE = "%s: affected %d rows of %d";

    protected final WinningTransactionDao winningTransactionDao;
    private final CitizenRankingDao citizenRankingDao;
    private final AggregatorStrategy aggregatorStrategy;

    public CashbackUpdateStrategyTemplate(WinningTransactionDao winningTransactionDao,
                                          CitizenRankingDao citizenRankingDao,
                                          AggregatorStrategy aggregatorStrategy) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyTemplate.CashbackUpdateStrategyTemplate");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionDao = {}, citizenRankingDao = {}", winningTransactionDao, citizenRankingDao);
        }

        this.winningTransactionDao = winningTransactionDao;
        this.citizenRankingDao = citizenRankingDao;
        this.aggregatorStrategy = aggregatorStrategy;
    }

    @Override
    @Transactional("chainedTransactionManager")
    public int process(AwardPeriod awardPeriod, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyTemplate.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, simplePageRequest = {}", awardPeriod, simplePageRequest);
        }

        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(), simplePageRequest.getSize());
        List<WinningTransaction> transactions = retrieveTransactions(awardPeriod.getAwardPeriodId(), pageRequest);

        List<CitizenRanking> rankings = new ArrayList<>(aggregatorStrategy.aggregate(awardPeriod, transactions));

        if (!rankings.isEmpty()) {
            int[] affectedRows = citizenRankingDao.updateCashback(rankings);

            if (affectedRows.length != rankings.size()) {
                String message = String.format(ERROR_MESSAGE_TEMPLATE, "updateCashback", affectedRows.length, rankings.size());
                log.error(message);
                throw new CashbackUpdateException(message);

            } else {
                ArrayList<CitizenRanking> failedUpdateRankings = new ArrayList<>();

                for (int i = 0; i < affectedRows.length; i++) {

                    if (affectedRows[i] < 1) {
                        rankings.get(i).setInsertDate(rankings.get(i).getUpdateDate());
                        rankings.get(i).setInsertUser(rankings.get(i).getUpdateUser());
                        rankings.get(i).setUpdateDate(null);
                        rankings.get(i).setUpdateUser(null);
                        failedUpdateRankings.add(rankings.get(i));
                    }
                }

                if (!failedUpdateRankings.isEmpty()) {
                    affectedRows = citizenRankingDao.insertCashback(failedUpdateRankings);
                    checkErrors(failedUpdateRankings.size(), affectedRows, "insertCashback");
                }
            }
        }

        if (!transactions.isEmpty()) {
            int[] affectedRows = winningTransactionDao.updateProcessedTransaction(transactions);
            checkErrors(transactions.size(), affectedRows, "updateProcessedTransaction");
        }

        return transactions.size();
    }


    protected abstract List<WinningTransaction> retrieveTransactions(long awardPeriodId, Pageable pageable);


    private void checkErrors(int statementsCount, int[] affectedRows, String operationName) {
        if (affectedRows.length != statementsCount) {
            String message = String.format(ERROR_MESSAGE_TEMPLATE, operationName, affectedRows.length, statementsCount);
            log.error(message);
            throw new CashbackUpdateException(message);

        } else {
            long failedUpdateCount = Arrays.stream(affectedRows)
                    .filter(DaoHelper.isStatementResultKO)
                    .count();

            if (failedUpdateCount > 0) {
                String message = String.format(ERROR_MESSAGE_TEMPLATE, operationName, statementsCount - failedUpdateCount, statementsCount);
                log.error(message);
                throw new CashbackUpdateException(message);
            }
        }
    }

}
