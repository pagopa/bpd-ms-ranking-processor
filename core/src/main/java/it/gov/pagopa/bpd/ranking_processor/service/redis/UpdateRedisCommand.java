package it.gov.pagopa.bpd.ranking_processor.service.redis;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;

/**
 * {@link RankingSubProcessCommand} implementation for Update Ranking subprocess
 */
@Slf4j
@Service
@ConditionalOnProperty(value = "redis-update.enable", havingValue = "true")
@Order
class UpdateRedisCommand implements RankingSubProcessCommand {

    private static final String MESSAGE = "Failed to update Redis table";
    private static final String FAILED_UPDATE_WORKER_MESSAGE_FORMAT = "Failed to %s worker to process %s";

    private final CitizenRankingDao citizenRankingDao;

    @Autowired
    public UpdateRedisCommand(CitizenRankingDao citizenRankingDao) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateRedisCommand.UpdateRedisCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }

        this.citizenRankingDao = citizenRankingDao;
    }


    @Override
    public void execute(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateRedisCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriod = {}", awardPeriod);
        }

        if (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK) == 0
                && citizenRankingDao.getWorkerCount(UPDATE_RANKING) == 0) {
            int affectedRow = citizenRankingDao.registerWorker(UPDATE_REDIS, true);
            if (affectedRow != 0) {
                checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "register", UPDATE_REDIS));

                try {
                    affectedRow = citizenRankingDao.updateRedis();
                    checkError(affectedRow, MESSAGE);

                } catch (RuntimeException e) {
                    log.error(e.getMessage());
                    unregisterWorker();
                    throw e;
                }

                unregisterWorker();

            } else {
                log.info("skip sub process");
            }
        }
    }

    private void unregisterWorker() {
        int affectedRow = citizenRankingDao.unregisterWorker(UPDATE_REDIS);
        checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "unregister", UPDATE_REDIS));
    }

    private void checkError(int affectedRow, String message) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            log.error(message);
            throw new RedisUpdateException(message);
        }
    }

}
