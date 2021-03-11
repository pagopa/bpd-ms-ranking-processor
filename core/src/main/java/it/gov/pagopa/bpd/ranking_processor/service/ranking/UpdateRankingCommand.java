package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy.RankingUpdateStrategy;
import it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy.RankingUpdateStrategyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_CASHBACK;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_RANKING;

/**
 * {@link RankingSubProcessCommand} implementation for Update Ranking subprocess
 */
@Slf4j
@Service
@Order(2)
class UpdateRankingCommand implements RankingSubProcessCommand {

    public static final String FAILED_UPDATE_WORKER_MESSAGE_FORMAT = "Failed to %s worker to process %s";
    private final RankingUpdateStrategyFactory rankingUpdateStrategyFactory;
    private final int rankingUpdateLimit;
    private final CitizenRankingDao citizenRankingDao;

    @Autowired
    public UpdateRankingCommand(RankingUpdateStrategyFactory rankingUpdateStrategyFactory,
                                CitizenRankingDao citizenRankingDao,
                                @Value("${ranking-update.data-extraction.limit}") int rankingUpdateLimit) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateRankingCommand.UpdateRankingCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingUpdateStrategyFactory = {}, rankingUpdateLimit = {}", rankingUpdateStrategyFactory, rankingUpdateLimit);
        }

        this.rankingUpdateStrategyFactory = rankingUpdateStrategyFactory;
        this.citizenRankingDao = citizenRankingDao;
        this.rankingUpdateLimit = rankingUpdateLimit;
    }


    @Override
    public void execute(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateRankingCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        if (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK) == 0) {
            int affectedRow = citizenRankingDao.registerWorker(UPDATE_RANKING, true);
            if (affectedRow != 0) {
                checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "register", UPDATE_RANKING));

                try {
                    exec(awardPeriod);

                } catch (RuntimeException e) {
                    log.error(e.getMessage());
                    unregisterWorker();
                    throw e;
                }
                unregisterWorker();

            } else {
                log.info("skip {}", UPDATE_RANKING);
            }
        }
    }


    private void exec(AwardPeriod awardPeriod) {
        int pageNumber = 0;
        int citizensCount;
        RankingUpdateStrategy rankingUpdateStrategy = rankingUpdateStrategyFactory.create();

        do {
            SimplePageRequest pageRequest = SimplePageRequest.of(pageNumber++, rankingUpdateLimit);
            log.info("Start {} with page {}", rankingUpdateStrategy.getClass().getSimpleName(), pageRequest);
            citizensCount = rankingUpdateStrategy.process(awardPeriod.getAwardPeriodId(), pageRequest);
            log.info("End {} with page {}", rankingUpdateStrategy.getClass().getSimpleName(), pageRequest);

        } while (citizensCount >= rankingUpdateLimit);

        rankingUpdateStrategy.updateRankingExt(awardPeriod);
    }


    private void unregisterWorker() {
        int affectedRow = citizenRankingDao.unregisterWorker(UPDATE_RANKING);
        checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "unregister", UPDATE_RANKING));
    }


    private void checkError(int affectedRow, String message) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            log.error(message);
            throw new RankingUpdateException(message);
        }
    }

}
