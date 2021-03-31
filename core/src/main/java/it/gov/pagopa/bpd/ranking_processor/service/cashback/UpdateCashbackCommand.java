package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType.PARTIAL_TRANSFER;

/**
 * {@link RankingSubProcessCommand} implementation for Update Cashback subprocess
 */
@Slf4j
@Service
@Order(1)
class UpdateCashbackCommand implements RankingSubProcessCommand {

    private final CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory;
    private final int cashbackUpdateLimit;
    private final int cashbackUpdateDeadlockRetry;
    private final CitizenRankingDao citizenRankingDao;


    @Autowired
    public UpdateCashbackCommand(CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory,
                                 CitizenRankingDao citizenRankingDao,
                                 @Value("${cashback-update.data-extraction.limit}") int cashbackUpdateLimit,
                                 @Value("${cashback-update.deadlock-retry.limit}") Integer cashbackUpdateDeadlockRetry) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.UpdateCashbackCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("cashbackUpdateStrategyFactory = {}, citizenRankingDao = {}, cashbackUpdateLimit = {}, cashbackUpdateDeadlockRetry = {}", cashbackUpdateStrategyFactory, citizenRankingDao, cashbackUpdateLimit, cashbackUpdateDeadlockRetry);
        }
        if (cashbackUpdateDeadlockRetry != null && cashbackUpdateDeadlockRetry < 0) {
            throw new IllegalArgumentException("deadlock retry limit must be a positive integer");
        }

        this.cashbackUpdateStrategyFactory = cashbackUpdateStrategyFactory;
        this.citizenRankingDao = citizenRankingDao;
        this.cashbackUpdateLimit = cashbackUpdateLimit;
        this.cashbackUpdateDeadlockRetry = cashbackUpdateDeadlockRetry == null ? Integer.MAX_VALUE : cashbackUpdateDeadlockRetry;
    }


    @Override
    public void execute(AwardPeriod awardPeriod, LocalDateTime stopDateTime) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        registerWorker(UPDATE_CASHBACK, false);

        for (TransactionType trxType : TransactionType.values()) {

            if (PARTIAL_TRANSFER.equals(trxType)
                    && (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PAYMENT) > 0
                    || citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_TOTAL_TRANSFER) > 0)) {
                log.info("skip {}", UPDATE_CASHBACK_PARTIAL_TRANSFER);
                continue;
            }

            registerWorker(getUpdateRankingSubProcess(trxType), PARTIAL_TRANSFER.equals(trxType));

            try {
                exec(awardPeriod, trxType, stopDateTime);

            } catch (RuntimeException e) {
                log.error(e.getMessage());
                unregisterWorker(getUpdateRankingSubProcess(trxType));
                unregisterWorker(UPDATE_CASHBACK);
                throw e;
            }

            unregisterWorker(getUpdateRankingSubProcess(trxType));
        }

        unregisterWorker(UPDATE_CASHBACK);
    }


    private RankingProcess getUpdateRankingSubProcess(TransactionType trxType) {
        return valueOf(UPDATE_CASHBACK.name() + "_" + trxType.name());
    }


    private void exec(AwardPeriod awardPeriod, TransactionType trxType, LocalDateTime stopDateTime) {
        CashbackUpdateStrategy cashbackUpdateStrategy = cashbackUpdateStrategyFactory.create(trxType);

        if (cashbackUpdateStrategy == null) {
            log.info("skip {}", getUpdateRankingSubProcess(trxType));

        } else {
            int trxCount = cashbackUpdateLimit;
            while (trxCount == cashbackUpdateLimit && !isToStop.test(stopDateTime)) {

                SimplePageRequest pageRequest = SimplePageRequest.of(0, cashbackUpdateLimit);
                log.info("Start {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);

                int retryCount = 0;
                while (retryCount < cashbackUpdateDeadlockRetry && !isToStop.test(stopDateTime)) {

                    try {
                        trxCount = cashbackUpdateStrategy.process(awardPeriod, pageRequest);
                        break;

                    } catch (DeadlockLoserDataAccessException e) {
                        log.warn(e.getMessage());
                        retryCount++;
                    }
                }

                log.info("End {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);
            }
        }
    }


    private void registerWorker(RankingProcess process, boolean exclusiveLock) {
        int affectedRow = citizenRankingDao.registerWorker(process, exclusiveLock);
        checkError(affectedRow, "register", process);
    }


    private void checkError(int affectedRow, String action, RankingProcess process) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            String message = String.format("Failed to %s worker to process %s", action, process);
            log.error(message);
            throw new CashbackUpdateException(message);
        }
    }


    private void unregisterWorker(RankingProcess process) {
        int affectedRow = citizenRankingDao.unregisterWorker(process);
        checkError(affectedRow, "unregister", process);
    }

}
