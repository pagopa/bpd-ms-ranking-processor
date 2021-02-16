package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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
public class ParallelRankingUpdateImpl extends RankingUpdateStrategyTemplate {


    @Autowired
    public ParallelRankingUpdateImpl(CitizenRankingDao citizenRankingDao) {
        super(citizenRankingDao);

        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdateImpl.ParallelRankingUpdateImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }
    }


    @Override
    protected void setRanking(Map<Long, Set<CitizenRanking>> tiedMap) {
        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdateImpl.setRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("tiedMap = {}", tiedMap);
        }

        for (Set<CitizenRanking> ties : tiedMap.values()) {

            int startRankingChunk = lastAssignedRanking.getValue() + 1;
            CitizenRanking[] tiesArray = ties.toArray(new CitizenRanking[ties.size()]);
            IntStream.range(startRankingChunk, startRankingChunk + tiesArray.length)
                    .parallel()
                    .forEach(i -> tiesArray[i - startRankingChunk].setRanking((long) i));
            lastAssignedRanking.add(tiesArray.length);
        }
    }


    @Override
    protected Map<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("ParallelRankingUpdateImpl.aggregateData");
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
