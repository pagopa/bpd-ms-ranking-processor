package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Parallel implementation of {@link RankingUpdateStrategyTemplate}
 */
@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class ParallelRankingUpdate extends RankingUpdateStrategyTemplate {


    @Autowired
    public ParallelRankingUpdate(CitizenRankingDao citizenRankingDao,
                                 @Value("${ranking-update.tie-break.enable}") boolean tieBreakEnabled,
                                 @Value("${ranking-update.tie-break.limit}") int tieBreakLimit) {
        super(citizenRankingDao, tieBreakEnabled, tieBreakLimit);

        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdate.ParallelRankingUpdate");
        }
    }


    @Override
    protected void setRanking(Map<Long, Set<CitizenRanking>> tiedMap, AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdate.setRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("tiedMap = {}", tiedMap);
        }

        for (Set<CitizenRanking> ties : tiedMap.values()) {

            int startRankingChunk = lastAssignedRanking + 1;
            CitizenRanking[] tiesArray = ties.toArray(new CitizenRanking[ties.size()]);
            IntStream.range(startRankingChunk, startRankingChunk + tiesArray.length)
                    .parallel()
                    .forEach(i -> {
                        CitizenRanking citizenRanking = tiesArray[i - startRankingChunk];
                        citizenRanking.setRanking((long) i);
                        citizenRanking.setUpdateDate(startProcess);
                        citizenRanking.setUpdateUser(RankingProcessorService.PROCESS_NAME);
                        if (citizenRanking.getRanking() <= awardPeriod.getMinPosition()) {
                            lastMinTransactionNumber.updateAndGet(operand -> {
                                if (citizenRanking.getTransactionNumber() < operand)
                                    return citizenRanking.getTransactionNumber().intValue();
                                else
                                    return operand;
                            });
                        }
                    });
            lastAssignedRanking += tiesArray.length;
        }
    }


    @Override
    protected NavigableMap<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdate.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        return citizenRankings.stream()
                .parallel()
                .collect(Collectors.groupingByConcurrent(CitizenRanking::getTransactionNumber,
                        () -> new ConcurrentSkipListMap<>(Comparator.reverseOrder()),
                        Collectors.toCollection(() -> new TreeSet<>(tieBreak))));
    }

}
