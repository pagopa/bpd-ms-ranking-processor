package it.gov.pagopa.bpd.ranking_processor.service.milestone;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@EqualsAndHashCode(callSuper = false)
@Data
@AllArgsConstructor
@Slf4j
public class ConcurrentJob implements Callable<Void> {
    private final AtomicInteger totalCitizenElab;
    private final AtomicBoolean checkContinueUpdateRankingMilestone;
    private final Integer maxRecordToUpdate;
    private final int milestoneUpdateLimit;
    private final CitizenRankingDao citizenRankingDao;
    private final OffsetDateTime timestamp;

    @Override
    public Void call() {
        if (checkContinueUpdateRankingMilestone.get()
                && (maxRecordToUpdate == null
                || totalCitizenElab.get() < maxRecordToUpdate)) {
            Integer citizenElab = citizenRankingDao.updateMilestone(0, milestoneUpdateLimit, timestamp);
            totalCitizenElab.addAndGet(citizenElab);
            if (log.isDebugEnabled()) {
                log.debug("Citizen elaborated: {}", citizenElab);
                log.debug("Total citizen elaborated: {}", totalCitizenElab);
            }
            checkContinueUpdateRankingMilestone.set(citizenElab == milestoneUpdateLimit);
            call();
        }

        return null;
    }
}
