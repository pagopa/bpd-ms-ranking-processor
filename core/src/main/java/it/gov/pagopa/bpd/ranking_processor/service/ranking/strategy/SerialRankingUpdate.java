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
import java.util.stream.Collectors;

/**
 * Serial implementation of {@link RankingUpdateStrategyTemplate}
 */
@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class SerialRankingUpdate extends RankingUpdateStrategyTemplate {

    @Autowired
    public SerialRankingUpdate(CitizenRankingDao citizenRankingDao,
                               @Value("${ranking-update.tie-break.enable}") boolean tieBreakEnabled,
                               @Value("${ranking-update.tie-break.limit}") int tieBreakLimit) {
        super(citizenRankingDao, tieBreakEnabled, tieBreakLimit);

        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdate.SerialRankingUpdate");
        }
    }


    @Override
    protected void setRanking(Map<Long, Set<CitizenRanking>> tiedMap, AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdate.setRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("tiedMap = {}", tiedMap);
        }

        for (Set<CitizenRanking> ties : tiedMap.values()) {

            for (CitizenRanking citizenRanking : ties) {
                citizenRanking.setRanking((long) ++lastAssignedRanking);
                citizenRanking.setUpdateDate(startProcess);
                citizenRanking.setUpdateUser(RankingProcessorService.PROCESS_NAME);

                if (citizenRanking.getRanking() <= awardPeriod.getMinPosition()) {
                    lastMinTransactionNumber.set(citizenRanking.getTransactionNumber().intValue());
                }
            }
        }
    }


    @Override
    protected NavigableMap<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("SerialRankingUpdate.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        return citizenRankings.stream()
                .collect(Collectors.groupingBy(CitizenRanking::getTransactionNumber,
                        () -> new TreeMap<>(Comparator.reverseOrder()),
                        Collectors.toCollection(() -> new TreeSet<>(tieBreak))));
    }

}
