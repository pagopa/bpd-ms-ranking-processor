package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * See {@link RankingUpdateService}
 */
@Slf4j
@Service
public class RankingUpdateServiceImpl implements RankingUpdateService {

    private final CitizenRankingDao citizenRankingDao;
    private static final Comparator<CitizenRanking> tieBreak = Comparator.comparing(CitizenRanking::getFiscalCode);
    private final boolean parallelEnabled;


    @Autowired
    public RankingUpdateServiceImpl(CitizenRankingDao citizenRankingDao,
                                    @Value("${ranking-update.parallel.enable}") boolean parallelEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateServiceImpl.RankingUpdateServiceImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}, parallelEnabled = {}", citizenRankingDao, parallelEnabled);
        }

        this.citizenRankingDao = citizenRankingDao;
        this.parallelEnabled = parallelEnabled;
    }


    @Override
    @Transactional
    public int process(long awardPeriodId, MutableInt lastAssignedRanking, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, lastAssignedRanking = {}, simplePageRequest = {}", awardPeriodId, lastAssignedRanking, simplePageRequest);
        }

        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(),
                simplePageRequest.getSize(),
                CitizenRankingDao.SORT_BY_TRX_NUM_DESC);
        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriodId, pageRequest);

        setRanking(lastAssignedRanking, citizenRankings);

        int totalExtractedRankings = pageRequest.getPageNumber() * pageRequest.getPageSize() + citizenRankings.size();
        if (!lastAssignedRanking.getValue().equals(totalExtractedRankings)) {
            throw new IllegalStateException(String.format("Size of processed ranking records (%d) differs from the extracted one (%d)",
                    lastAssignedRanking.getValue(),
                    totalExtractedRankings));
        }

        int[] affectedRows = citizenRankingDao.updateRanking(citizenRankings);
        if (affectedRows.length != citizenRankings.size()) {
            log.warn("updateRanking: affected {} rows of {}", affectedRows.length, citizenRankings.size());
        }

        return citizenRankings.size();
    }


    private void setRanking(MutableInt lastAssignedRanking, List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateServiceImpl.setRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("lastAssignedRanking = {}, citizenRankings = {}", lastAssignedRanking, citizenRankings);
        }

        Map<Long, Set<CitizenRanking>> tiedMap = aggregateData(citizenRankings);

        for (Set<CitizenRanking> ties : tiedMap.values()) {

            if (parallelEnabled) {
                int startRankingChunk = lastAssignedRanking.getValue() + 1;
                CitizenRanking[] tiesArray = ties.toArray(new CitizenRanking[ties.size()]);
                IntStream.range(startRankingChunk, startRankingChunk + tiesArray.length)
                        .parallel()
                        .forEach(i -> tiesArray[i - startRankingChunk].setRanking((long) i));
                lastAssignedRanking.add(tiesArray.length);

            } else {
                for (CitizenRanking citizenRanking : ties) {
                    citizenRanking.setRanking((long) lastAssignedRanking.incrementAndGet());
                }
            }
        }
    }


    private Map<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateServiceImpl.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        Map<Long, Set<CitizenRanking>> tiedMap;

        if (parallelEnabled) {
            tiedMap = citizenRankings.stream()
                    .parallel()
                    .collect(Collectors.groupingByConcurrent(CitizenRanking::getTransactionNumber,
                            () -> new ConcurrentSkipListMap<>(Comparator.reverseOrder()),
                            Collectors.toCollection(() -> new TreeSet<>(tieBreak))));

        } else {
            tiedMap = citizenRankings.stream()
                    .collect(Collectors.groupingBy(CitizenRanking::getTransactionNumber,
                            () -> new TreeMap<>(Comparator.reverseOrder()),
                            Collectors.toCollection(() -> new TreeSet<>(tieBreak))));
        }

        return tiedMap;
    }

}
