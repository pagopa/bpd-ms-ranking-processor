package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
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
    public ParallelRankingUpdate(CitizenRankingDao citizenRankingDao) {
        super(citizenRankingDao);

        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdate.ParallelRankingUpdate");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
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

        OffsetDateTime now = OffsetDateTime.now();
        for (Set<CitizenRanking> ties : tiedMap.values()) {

            int startRankingChunk = lastAssignedRanking.getValue() + 1;
            CitizenRanking[] tiesArray = ties.toArray(new CitizenRanking[ties.size()]);
            IntStream.range(startRankingChunk, startRankingChunk + tiesArray.length)
                    .parallel()
                    .forEach(i -> {
                        CitizenRanking citizenRanking = tiesArray[i - startRankingChunk];
                        citizenRanking.setRanking((long) i);
                        citizenRanking.setUpdateDate(now);
                        citizenRanking.setUpdateUser(RankingProcessorService.PROCESS_NAME);
                        if (citizenRanking.getRanking() <= awardPeriod.getMinPosition()
                                && minTransactionNumber < citizenRanking.getTransactionNumber()) {
                            minTransactionNumber = citizenRanking.getTransactionNumber().intValue();
                        }
                    });
            lastAssignedRanking.add(tiesArray.length);
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
                        Collectors.toCollection(() -> new TreeSet<>(TIE_BREAK))));
    }

}
