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

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType.PARTIAL_TRANSFER;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType.TOTAL_TRANSFER;

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

            if (TOTAL_TRANSFER.equals(trxType)
                    && totalTransferSingleProcessEnabled
                    && citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PAYMENT) > 0) {
                log.info("skip {}", UPDATE_CASHBACK_TOTAL_TRANSFER);
                continue;
            }

            if (PARTIAL_TRANSFER.equals(trxType)
                    && (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_PAYMENT) > 0
                    || citizenRankingDao.getWorkerCount(UPDATE_CASHBACK_TOTAL_TRANSFER) > 0)) {
                log.info("skip {}", UPDATE_CASHBACK_PARTIAL_TRANSFER);
                continue;
            }

            registerWorker(getUpdateRankingSubProcess(trxType), PARTIAL_TRANSFER.equals(trxType));

            try {
                exec(awardPeriod, trxType, stopTime);

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


    private void exec(AwardPeriod awardPeriod, TransactionType trxType, LocalTime stopTime) {
        CashbackUpdateStrategy cashbackUpdateStrategy = cashbackUpdateStrategyFactory.create(trxType);

        if (cashbackUpdateStrategy == null) {
            log.info("skip {}", getUpdateRankingSubProcess(trxType));

        } else {
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
                        if (retryCount++ < cashbackUpdateRetry) {
                            log.warn(e.getMessage());
                        } else {
                            throw new CashbackUpdateException("Exceeded max retry number");
                        }
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
