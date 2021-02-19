package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Serial implementation of {@link RankingUpdateStrategyTemplate}
 */
@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class SerialRankingUpdateImpl extends RankingUpdateStrategyTemplate {

    @Autowired
    public SerialRankingUpdateImpl(CitizenRankingDao citizenRankingDao) {
        super(citizenRankingDao);

        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdateImpl.SerialRankingUpdateImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }
    }


    @Override
    protected void setRanking(Map<Long, Set<CitizenRanking>> tiedMap) {
        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdateImpl.setRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("tiedMap = {}", tiedMap);
        }

        for (Set<CitizenRanking> ties : tiedMap.values()) {

            for (CitizenRanking citizenRanking : ties) {
                citizenRanking.setRanking((long) lastAssignedRanking.incrementAndGet());
            }
        }
    }


    @Override
    protected NavigableMap<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdateImpl.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        return citizenRankings.stream()
                .collect(Collectors.groupingBy(CitizenRanking::getTransactionNumber,
                        () -> new TreeMap<>(Comparator.reverseOrder()),
                        Collectors.toCollection(() -> new TreeSet<>(TIE_BREAK))));
    }

}
