package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Statement;
import java.util.*;

/**
 * Template Method pattern of {@link RankingUpdateStrategy}
 */
@Slf4j
abstract class RankingUpdateStrategyTemplate implements RankingUpdateStrategy {

    static final String ERROR_MESSAGE_TEMPLATE = "updateRanking: affected %d rows of %d";
    protected static final Comparator<CitizenRanking> TIE_BREAK = Comparator.comparing(CitizenRanking::getFiscalCode);

    protected final MutableInt lastAssignedRanking;

    private final CitizenRankingDao citizenRankingDao;
    /**
     * A set of latest (in terms of ranking) ties. Required to manage ties between each chunks
     */
    private Set<CitizenRanking> lastTies = Collections.emptySet();


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
                CitizenRankingDao.FIND_ALL_PAGEABLE_SORT);
        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriodId, pageRequest);
        int totalExtractedRankings = citizenRankings.size();

        citizenRankings.addAll(lastTies);
        NavigableMap<Long, Set<CitizenRanking>> tiedMap = aggregateData(citizenRankings);
        setRanking(tiedMap);

        if (lastAssignedRanking.intValue() != totalExtractedRankings) {
            throw new IllegalStateException(String.format("Size of processed ranking records (%d) differs from the extracted one (%d)",
                    lastAssignedRanking.intValue(),
                    totalExtractedRankings));
        }

        int[] affectedRows = citizenRankingDao.updateRanking(citizenRankings);
        checkErrors(citizenRankings.size(), affectedRows);

        lastTies = tiedMap.lastEntry().getValue();
        lastAssignedRanking.subtract(lastTies.size());

        return totalExtractedRankings;
    }


    private void checkErrors(int statementsCount, int[] affectedRows) {
        if (affectedRows.length != statementsCount) {
            String message = String.format(ERROR_MESSAGE_TEMPLATE, affectedRows.length, statementsCount);
            log.error(message);
            throw new RankingUpdateException(message);

        } else {
            long failedUpdateCount = Arrays.stream(affectedRows)
                    .filter(value -> value != 1 && value != Statement.SUCCESS_NO_INFO)
                    .count();

            if (failedUpdateCount > 0) {
                String message = String.format(ERROR_MESSAGE_TEMPLATE, statementsCount - failedUpdateCount, statementsCount);
                log.error(message);
                throw new RankingUpdateException(message);
            }
        }
    }


    protected abstract void setRanking(Map<Long, Set<CitizenRanking>> tiedMap);

    protected abstract NavigableMap<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings);

}
