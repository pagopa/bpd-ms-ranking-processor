package it.gov.pagopa.bpd.ranking_processor.service.milestone;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;

/**
 * {@link RankingSubProcessCommand} implementation for Update Milestone subprocess
 */
@Slf4j
@Service
@Conditional(MilestoneUpdateEnabledCondition.class)
@Order(3)
class UpdateMilestoneCommand implements RankingSubProcessCommand {

    private static final String FAILED_UPDATE_WORKER_MESSAGE_FORMAT = "Failed to %s worker to process %s";

    private final CitizenRankingDao citizenRankingDao;
    private final int threadPool;
    private final Integer maxRecordToUpdate;
    private final int milestoneUpdateLimit;


    @Autowired
    public UpdateMilestoneCommand(CitizenRankingDao citizenRankingDao,
                                  @Value("${milestone-update.thread-pool-size}") int threadPool,
                                  @Value("${milestone-update.max-record-to-update}") Integer maxRecordToUpdate,
                                  @Value("${milestone-update.data-extraction.limit}") int milestoneUpdateLimit) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateMilestoneCommand.UpdateMilestoneCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }

        this.citizenRankingDao = citizenRankingDao;
        this.threadPool = threadPool;
        this.maxRecordToUpdate = maxRecordToUpdate;
        this.milestoneUpdateLimit = milestoneUpdateLimit;
    }


    @Override
    public void execute(AwardPeriod awardPeriod, LocalTime stopTime) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateMilestoneCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriod = {}", awardPeriod);
        }

        if (!isToStop.test(stopTime)
                && citizenRankingDao.getWorkerCount(UPDATE_CASHBACK) == 0
                && citizenRankingDao.getWorkerCount(UPDATE_RANKING) == 0) {
            int affectedRow = citizenRankingDao.registerWorker(UPDATE_MILESTONE, true);
            if (affectedRow != 0) {
                checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "register", UPDATE_MILESTONE));

                try {
                    updateRankingMilestone();

                } catch (RuntimeException e) {
                    log.error(e.getMessage());
                    unregisterWorker();
                    throw e;
                }

                unregisterWorker();

            } else {
                log.info("skip {}", UPDATE_MILESTONE);
            }
        }
    }


    private void unregisterWorker() {
        int affectedRow = citizenRankingDao.unregisterWorker(UPDATE_MILESTONE);
        checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "unregister", UPDATE_MILESTONE));
    }


    private void checkError(int affectedRow, String message) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            log.error(message);
            throw new MilestoneUpdateException(message);
        }
    }


    private void updateRankingMilestone() {
        log.info("Start execute updateRankingMilestone");

        ExecutorService pool = null;
        AtomicBoolean checkContinueUpdateRankingMilestone = new AtomicBoolean(true);
        AtomicInteger totalCitizenElab = new AtomicInteger(0);

        try {
            OffsetDateTime timestamp = OffsetDateTime.now();

            pool = Executors.newFixedThreadPool(threadPool);
            ArrayList<ConcurrentJob> concurrentJobs = new ArrayList<>(threadPool);
            for (int threadCount = 0; threadCount < threadPool; threadCount++) {
                concurrentJobs.add(new ConcurrentJob(totalCitizenElab,
                        checkContinueUpdateRankingMilestone,
                        maxRecordToUpdate,
                        milestoneUpdateLimit,
                        citizenRankingDao,
                        timestamp));
            }
            pool.invokeAll(concurrentJobs);

        } catch (InterruptedException e) {
            throw new MilestoneUpdateException(e.getMessage());

        } finally {
            if (pool != null) {
                pool.shutdown();
            }
        }

        log.info("End execute updateRankingMilestone");
    }

}
