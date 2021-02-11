package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Slf4j
@Service
public class RankingUpdateServiceImpl implements RankingUpdateService {

    private final CitizenRankingDao citizenRankingDao;
    private final int limit;
    private final Comparator<CitizenRanking> tieBreak = Comparator.comparing(CitizenRanking::getFiscalCode);
    private final boolean isParallel = true;
    private final boolean parallelEnabled;

    @Autowired
    public RankingUpdateServiceImpl(CitizenRankingDao citizenRankingDao,
                                    @Value("${ranking-update.data-extraction.limit}") int limit,
                                    @Value("${ranking-update.parallel.enable}") boolean parallelEnabled) {
        this.citizenRankingDao = citizenRankingDao;
        this.limit = limit;
        this.parallelEnabled = parallelEnabled;
    }

    public void process(final Long awardPeriodId) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }

        int pageNumber = 0;
        int trxCount;
        do {
            trxCount = process(awardPeriodId, pageNumber++);
        } while (limit == trxCount);
    }

    @Override
    public int process(Long awardPeriodId, int pageNumber) {
        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }

        Pageable pageRequest = PageRequest.of(limit, pageNumber, CitizenRankingDao.SORT_BY_TRX_NUM_DESC);
        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriodId, pageRequest);

        Map<Long, Set<CitizenRanking>> tiedMap;
        if (parallelEnabled) {
            tiedMap = citizenRankings.stream()
                    .parallel()
                    .collect(Collectors.groupingByConcurrent(CitizenRanking::getTransactionNumber,
                            ConcurrentSkipListMap::new,
                            Collectors.toCollection(() -> new TreeSet<>(tieBreak))));
        } else {
            tiedMap = citizenRankings.stream()
                    .collect(Collectors.groupingBy(CitizenRanking::getTransactionNumber,
                            Collectors.toCollection(() -> new TreeSet<>(tieBreak))));
        }

        ArrayList<CitizenRanking> updatedRankings = new ArrayList<>(citizenRankings.size());
        long ranking = 1;

        for (Set<CitizenRanking> ties : tiedMap.values()) {
            if (parallelEnabled) {
                CitizenRanking[] tiesArray = ties.toArray(new CitizenRanking[ties.size()]);
                CopyOnWriteArrayList<CitizenRanking> updatedRankinsChunk = IntStream
                        .range(0, tiesArray.length)
                        .parallel()
                        .mapToObj(i -> {
                            tiesArray[i].setRanking((long) i);
                            return tiesArray[i];
                        })
                        .collect(CopyOnWriteArrayList::new, CopyOnWriteArrayList::add, CopyOnWriteArrayList::addAll);
                ranking = ranking + tiesArray.length;
                updatedRankings.addAll(updatedRankinsChunk);

            } else {
                for (CitizenRanking citizenRanking : ties) {
                    citizenRanking.setRanking(ranking++);
                }
                updatedRankings.addAll(ties);
            }
        }

        if (citizenRankings.size() != updatedRankings.size()) {
            throw new IllegalStateException("Size of processed citizen ranking records differs from the extracted one");
        }

        int[] affectedRows = citizenRankingDao.updateRanking(updatedRankings);
        if (affectedRows.length != updatedRankings.size()) {
            log.warn("updateRanking: affected {} rows of {}", affectedRows.length, updatedRankings.size());
        }

        return citizenRankings.size();
    }

}
