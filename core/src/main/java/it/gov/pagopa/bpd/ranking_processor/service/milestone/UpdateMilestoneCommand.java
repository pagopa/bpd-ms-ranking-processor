package it.gov.pagopa.bpd.ranking_processor.service.milestone;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
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
    private final boolean singleProcessEnabled;
    private final long waitTime;
    private final Integer milestoneUpdateRetry;


    @Autowired
    public UpdateMilestoneCommand(CitizenRankingDao citizenRankingDao,
                                  @Value("${milestone-update.thread-pool-size}") int threadPool,
                                  @Value("${milestone-update.max-record-to-update}") Integer maxRecordToUpdate,
                                  @Value("${milestone-update.data-extraction.limit}") int milestoneUpdateLimit,
                                  @Value("${milestone-update.single-process.enable}") boolean singleProcessEnabled,
                                  @Value("${milestone-update.wait-time}") long waitTime,
                                  @Value("${milestone-update.retry.limit}") Integer milestoneUpdateRetry) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateMilestoneCommand.UpdateMilestoneCommand");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }

        this.citizenRankingDao = citizenRankingDao;
        this.threadPool = threadPool;
        this.maxRecordToUpdate = maxRecordToUpdate == null ? Integer.MAX_VALUE : maxRecordToUpdate;
        this.milestoneUpdateLimit = milestoneUpdateLimit;
        this.singleProcessEnabled = singleProcessEnabled;
        this.waitTime = waitTime;
        this.milestoneUpdateRetry = milestoneUpdateRetry == null ? Integer.MAX_VALUE : milestoneUpdateRetry;
    }


    @Override
    public void execute(AwardPeriod awardPeriod, LocalTime stopTime) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateMilestoneCommand.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriod = {}", awardPeriod);
        }

        if (!isToStop.test(stopTime)) {
            boolean retry;
            do {
                retry = false;

                if (citizenRankingDao.getWorkerCount(UPDATE_CASHBACK) == 0
                        && citizenRankingDao.getWorkerCount(UPDATE_RANKING) == 0) {
                    int affectedRow = citizenRankingDao.registerWorker(UPDATE_MILESTONE, singleProcessEnabled);
                    if (affectedRow != 0) {
                        checkError(affectedRow, String.format(FAILED_UPDATE_WORKER_MESSAGE_FORMAT, "register", UPDATE_MILESTONE));

                        try {
                            updateRankingMilestone(stopTime);

                        } catch (RuntimeException e) {
                            log.error(e.getMessage());
                            unregisterWorker();
                            throw e;
                        }

                        unregisterWorker();

                    } else {
                        log.info("skip {}", UPDATE_MILESTONE);
                    }

                } else {
                    if (!singleProcessEnabled) {
                        try {
                            Thread.sleep(waitTime);

                        } catch (InterruptedException e) {
                            throw new MilestoneUpdateException(e.getMessage());
                        }
                        retry = true;
                    }
                }

            } while (retry);
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


    private void updateRankingMilestone(LocalTime stopTime) {
        log.info("Start execute updateRankingMilestone");

        ExecutorService pool = null;
        AtomicBoolean checkContinueUpdateRankingMilestone = new AtomicBoolean(true);
        AtomicInteger totalCitizenElab = new AtomicInteger(0);

        try {
            OffsetDateTime timestamp = OffsetDateTime.now();

            pool = Executors.newFixedThreadPool(threadPool);
            ArrayList<Callable<Void>> concurrentJobs = new ArrayList<>(threadPool);
            Map<String, String> mdcContextMap = MDC.getCopyOfContextMap();
            for (int threadCount = 0; threadCount < threadPool; threadCount++) {
                concurrentJobs.add(new Callable<Void>() {
                    private int retryCount = 0;

                    {
                        if (mdcContextMap == null) {
                            MDC.clear();
                        } else {
                            MDC.setContextMap(mdcContextMap);
                        }
                    }

                    @Override
                    public Void call() {
                        if (!isToStop.test(stopTime)
                                && checkContinueUpdateRankingMilestone.get()
                                && (maxRecordToUpdate == null
                                || totalCitizenElab.get() < maxRecordToUpdate)) {
                            log.info("Start updateMilestone chunk with limit {}",
                                    milestoneUpdateLimit);
                            try {
                                int citizenElab = citizenRankingDao.updateMilestone(0, milestoneUpdateLimit, timestamp);
                                totalCitizenElab.addAndGet(citizenElab);
                                log.info("End updateMilestone chunk: citizen elaborated = {}, total citizen elaborated = {}",
                                        citizenElab,
                                        totalCitizenElab);
                                checkContinueUpdateRankingMilestone.set(citizenElab == milestoneUpdateLimit);
                            } catch (DeadlockLoserDataAccessException e) {
                                log.warn(e.getMessage());
                                if (++retryCount > milestoneUpdateRetry) {
                                    log.error("Exceeded max retry number");
                                    throw e;
                                }
                            }
                            call();
                        }
                        return null;
                    }
                });
            }
            List<Future<Void>> futures = pool.invokeAll(concurrentJobs);
            for (Future<Void> future : futures) {
                future.get();
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new MilestoneUpdateException(e.getMessage());

        } finally {
            if (pool != null) {
                pool.shutdown();
            }
        }

        log.info("End execute updateRankingMilestone");
    }

}
