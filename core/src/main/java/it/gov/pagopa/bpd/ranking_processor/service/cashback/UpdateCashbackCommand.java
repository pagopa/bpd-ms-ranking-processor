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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.EnumMap;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType.*;

/**
 * {@link RankingSubProcessCommand} implementation for Update Cashback subprocess
 */
@Slf4j
@Service
@Order(1)
class UpdateCashbackCommand implements RankingSubProcessCommand {

    private final CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory;
    private final int cashbackUpdateRetry;
    private final CitizenRankingDao citizenRankingDao;
    private final boolean totalTransferSingleProcessEnabled;
    private final EnumMap<TransactionType, RankingProcess> trxType2RankingProcessMap;


    @Autowired
    public UpdateCashbackCommand(CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory,
                                 CitizenRankingDao citizenRankingDao,
                                 @Value("${cashback-update.retry.limit}") Integer cashbackUpdateRetry,
                                 @Value("${cashback-update.total-transfer.single-process.enable}") boolean totalTransferSingleProcessEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.UpdateCashbackCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("cashbackUpdateStrategyFactory = {}, citizenRankingDao = {}, cashbackUpdateDeadlockRetry = {}", cashbackUpdateStrategyFactory, citizenRankingDao, cashbackUpdateRetry);
        }
        if (cashbackUpdateRetry != null && cashbackUpdateRetry < 0) {
            throw new IllegalArgumentException("retry limit must be a positive integer");
        }

        trxType2RankingProcessMap = new EnumMap<>(TransactionType.class);
        trxType2RankingProcessMap.put(PAYMENT, UPDATE_CASHBACK_PAYMENT);
        trxType2RankingProcessMap.put(TOTAL_TRANSFER, UPDATE_CASHBACK_TOTAL_TRANSFER);
        trxType2RankingProcessMap.put(PARTIAL_TRANSFER, UPDATE_CASHBACK_PARTIAL_TRANSFER);

        this.cashbackUpdateStrategyFactory = cashbackUpdateStrategyFactory;
        this.citizenRankingDao = citizenRankingDao;
        this.cashbackUpdateRetry = cashbackUpdateRetry == null ? Integer.MAX_VALUE : cashbackUpdateRetry;
        this.totalTransferSingleProcessEnabled = totalTransferSingleProcessEnabled;
    }


    @Override
    public void execute(AwardPeriod awardPeriod, LocalTime stopTime) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        registerWorker(UPDATE_CASHBACK, false);

        for (TransactionType trxType : TransactionType.values()) {

            CashbackUpdateStrategy cashbackUpdateStrategy = cashbackUpdateStrategyFactory.create(trxType);

            if (cashbackUpdateStrategy == null) {
                log.info("skip {}", getUpdateRankingSubProcess(trxType));

            } else {

                if (TOTAL_TRANSFER.equals(trxType) && totalTransferSingleProcessEnabled) {
                    if (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PAYMENT) > 0
                            || citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_TOTAL_TRANSFER) > 0) {
                        log.info("skip {}", trxType2RankingProcessMap.get(trxType));
                        continue;
                    }
                }

                if (PARTIAL_TRANSFER.equals(trxType)) {
                    if (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PAYMENT) > 0
                            || citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_TOTAL_TRANSFER) > 0
                            || citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PARTIAL_TRANSFER) > 0) {
                        log.info("skip {}", trxType2RankingProcessMap.get(trxType));
                        continue;
                    }
                }

                boolean exclusiveLock = PARTIAL_TRANSFER.equals(trxType)
                        || (TOTAL_TRANSFER.equals(trxType) && totalTransferSingleProcessEnabled);
                try {
                    registerWorker(getUpdateRankingSubProcess(trxType), exclusiveLock);
                } catch (CashbackUpdateExclusiveLockException e) {
                    log.info("skip {}", trxType2RankingProcessMap.get(trxType));
                    continue;
                }

                try {
                    exec(awardPeriod, cashbackUpdateStrategy, stopTime);

                } catch (RuntimeException e) {
                    log.error(e.getMessage());
                    unregisterWorker(getUpdateRankingSubProcess(trxType));
                    unregisterWorker(UPDATE_CASHBACK);
                    throw e;
                }

                unregisterWorker(getUpdateRankingSubProcess(trxType));
            }
        }

        unregisterWorker(UPDATE_CASHBACK);
    }


    private RankingProcess getUpdateRankingSubProcess(TransactionType trxType) {
        return RankingProcess.valueOf(UPDATE_CASHBACK.name() + "_" + trxType.name());
    }


    private void exec(AwardPeriod awardPeriod, CashbackUpdateStrategy cashbackUpdateStrategy, LocalTime stopTime) {
        int trxCount = cashbackUpdateStrategy.getDataExtractionLimit();

        while (trxCount == cashbackUpdateStrategy.getDataExtractionLimit() && !isToStop.test(stopTime)) {

            SimplePageRequest pageRequest = SimplePageRequest.of(0, cashbackUpdateStrategy.getDataExtractionLimit());
            log.info("Start {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);

            int retryCount = 0;
            while (!isToStop.test(stopTime)) {

                try {
                    trxCount = cashbackUpdateStrategy.process(awardPeriod, pageRequest);
                    break;

                } catch (DeadlockLoserDataAccessException | DuplicateKeyException e) {
                    log.warn(e.getMessage());
                    if (++retryCount > cashbackUpdateRetry) {
                        log.error("Exceeded max retry number");
                        return;
                    }
                }
            }

            log.info("End {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);
        }
    }


    private void registerWorker(RankingProcess process, boolean exclusiveLock) {
        int affectedRow = citizenRankingDao.registerWorker(process, exclusiveLock);
        if (exclusiveLock) {
            if (affectedRow == 0) {
                throw new CashbackUpdateExclusiveLockException();
            }
        } else {
            checkError(affectedRow, "register", process);
        }
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
