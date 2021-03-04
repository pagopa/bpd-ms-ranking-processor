package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
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
                if (DaoHelper.isStatementResultKO.test(affectedRow)) {
                    String message = "Failed to register worker to process " + UPDATE_RANKING;
                    log.error(message);
                    throw new RankingUpdateException(message);
                }

                int pageNumber = 0;
                int citizensCount;
                RankingUpdateStrategy rankingUpdateStrategy = rankingUpdateStrategyFactory.create();
                do {
                    SimplePageRequest pageRequest = SimplePageRequest.of(pageNumber++, rankingUpdateLimit);
                    citizensCount = rankingUpdateStrategy.process(awardPeriod.getAwardPeriodId(), pageRequest);
                } while (citizensCount >= rankingUpdateLimit);

                rankingUpdateStrategy.updateRankingExt(awardPeriod);

                affectedRow = citizenRankingDao.unregisterWorker(UPDATE_RANKING);
                if (DaoHelper.isStatementResultKO.test(affectedRow)) {
                    String message = "Failed to unregister worker to process " + UPDATE_RANKING;
                    log.error(message);
                    throw new RankingUpdateException(message);
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("skip sub process");
                }
            }
        }
    }

}
