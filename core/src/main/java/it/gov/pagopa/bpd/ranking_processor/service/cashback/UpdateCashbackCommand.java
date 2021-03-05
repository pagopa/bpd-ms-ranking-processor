package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
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
import org.springframework.stereotype.Service;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_CASHBACK;

/**
 * {@link RankingSubProcessCommand} implementation for Update Cashback subprocess
 */
@Slf4j
@Service
@Order(1)
class UpdateCashbackCommand implements RankingSubProcessCommand {

    private final CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory;
    private final int cashbackUpdateLimit;
    private final CitizenRankingDao citizenRankingDao;

    @Autowired
    public UpdateCashbackCommand(CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory,
                                 CitizenRankingDao citizenRankingDao,
                                 @Value("${cashback-update.data-extraction.limit}") int cashbackUpdateLimit) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.UpdateCashbackCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("cashbackUpdateStrategyFactory = {}, cashbackUpdateLimit = {}", cashbackUpdateStrategyFactory, cashbackUpdateLimit);
        }

        this.cashbackUpdateStrategyFactory = cashbackUpdateStrategyFactory;
        this.citizenRankingDao = citizenRankingDao;
        this.cashbackUpdateLimit = cashbackUpdateLimit;
    }

    @Override
    public void execute(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        registerWorker();

        try {
            for (TransactionType trxType : TransactionType.values()) {
                CashbackUpdateStrategy cashbackUpdateStrategy = cashbackUpdateStrategyFactory.create(trxType);
                int trxCount;
                do {
                    SimplePageRequest pageRequest = SimplePageRequest.of(0, cashbackUpdateLimit);
                    log.info("Start {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);
                    trxCount = cashbackUpdateStrategy.process(awardPeriod, trxType, pageRequest);
                    log.info("End {} with page {}", cashbackUpdateStrategy.getClass().getSimpleName(), pageRequest);
                } while (trxCount == cashbackUpdateLimit);
            }

        } catch (RuntimeException e) {
            log.error(e.getMessage());
            unregisterWorker();
            throw e;
        }

        unregisterWorker();
    }

    private void registerWorker() {
        int affectedRow = citizenRankingDao.registerWorker(UPDATE_CASHBACK);
        checkError(affectedRow, "register");
    }

    private void checkError(int affectedRow, String action) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            String message = String.format("Failed to %s worker to process %s", action, UPDATE_CASHBACK);
            log.error(message);
            throw new CashbackUpdateException(message);
        }
    }

    private void unregisterWorker() {
        int affectedRow = citizenRankingDao.unregisterWorker(UPDATE_CASHBACK);
        checkError(affectedRow, "unregister");
    }

}
