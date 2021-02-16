package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Template Method pattern of {@link RankingUpdateStrategy}
 */
@Slf4j
abstract class RankingUpdateStrategyTemplate implements RankingUpdateStrategy {

    protected static final Comparator<CitizenRanking> TIE_BREAK = Comparator.comparing(CitizenRanking::getFiscalCode);

    protected final MutableInt lastAssignedRanking;

    private final CitizenRankingDao citizenRankingDao;


    public RankingUpdateStrategyTemplate(CitizenRankingDao citizenRankingDao) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.RankingUpdateStrategyTemplate");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }

        this.citizenRankingDao = citizenRankingDao;
        this.lastAssignedRanking = new MutableInt(0);
    }


    @Override
    @Transactional
    public int process(long awardPeriodId, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, simplePageRequest = {}", awardPeriodId, simplePageRequest);
        }

        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(),
                simplePageRequest.getSize(),
                CitizenRankingDao.SORT_BY_TRX_NUM_DESC);
        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriodId, pageRequest);

        Map<Long, Set<CitizenRanking>> tiedMap = aggregateData(citizenRankings);
        setRanking(tiedMap);

        int totalExtractedRankings = pageRequest.getPageNumber() * pageRequest.getPageSize() + citizenRankings.size();
        if (lastAssignedRanking.intValue() != totalExtractedRankings) {
            throw new IllegalStateException(String.format("Size of processed ranking records (%d) differs from the extracted one (%d)",
                    lastAssignedRanking.intValue(),
                    totalExtractedRankings));
        }

        int[] affectedRows = citizenRankingDao.updateRanking(citizenRankings);
        if (affectedRows.length != citizenRankings.size()) {
            log.warn("updateRanking: affected {} rows of {}", affectedRows.length, citizenRankings.size());
        }

        return citizenRankings.size();
    }


    protected abstract void setRanking(Map<Long, Set<CitizenRanking>> tiedMap);

    protected abstract Map<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings);

}
