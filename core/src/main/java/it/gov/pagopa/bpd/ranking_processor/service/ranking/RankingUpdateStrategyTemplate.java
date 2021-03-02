package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Template Method pattern of {@link RankingUpdateStrategy}
 */
@Slf4j
abstract class RankingUpdateStrategyTemplate implements RankingUpdateStrategy {

    static final String ERROR_MESSAGE_TEMPLATE = "updateRanking: affected %d rows of %d";
    protected static final Comparator<CitizenRanking> TIE_BREAK = Comparator.comparing(CitizenRanking::getFiscalCode);

    protected final MutableInt lastAssignedRanking = new MutableInt(0);

    private Integer maxTransactionNumber;
    private Integer minTransactionNumber;
    private final CitizenRankingDao citizenRankingDao;
    /**
     * A set of latest (in terms of ranking) ties. Required to manage ties between each chunks
     */
    private Set<CitizenRanking> lastTies = Collections.emptySet();

    @Override
    @Transactional("citizenTransactionManager")
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

        if (lastAssignedRanking.intValue() != pageRequest.getOffset() + totalExtractedRankings) {
            throw new IllegalStateException(String.format("Size of processed ranking records (%d) differs from the extracted one (%d)",
                    lastAssignedRanking.intValue(),
                    pageRequest.getOffset() + totalExtractedRankings));
        }

        int[] affectedRows = citizenRankingDao.updateRanking(citizenRankings);
        checkErrors(citizenRankings.size(), affectedRows);

        lastTies = tiedMap.lastEntry().getValue();
        lastAssignedRanking.subtract(lastTies.size());

        if (maxTransactionNumber == null) {
            maxTransactionNumber = tiedMap.firstKey().intValue();
        }
        minTransactionNumber = tiedMap.lastKey().intValue();

        return totalExtractedRankings;
    }

    protected abstract void setRanking(Map<Long, Set<CitizenRanking>> tiedMap);


    public RankingUpdateStrategyTemplate(CitizenRankingDao citizenRankingDao) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.RankingUpdateStrategyTemplate");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankingDao = {}", citizenRankingDao);
        }

        this.citizenRankingDao = citizenRankingDao;
    }

    protected abstract NavigableMap<Long, Set<CitizenRanking>> aggregateData(List<CitizenRanking> citizenRankings);

    private void checkErrors(int statementsCount, int[] affectedRows) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.checkErrors");
        }
        if (log.isDebugEnabled()) {
            log.debug("statementsCount = {}, affectedRows = {}", statementsCount, Arrays.toString(affectedRows));
        }

        if (affectedRows.length != statementsCount) {
            String message = String.format(ERROR_MESSAGE_TEMPLATE, affectedRows.length, statementsCount);
            log.error(message);
            throw new RankingUpdateException(message);

        } else {
            long failedUpdateCount = Arrays.stream(affectedRows)
                    .filter(this::isStatementResultKO)
                    .count();

            if (failedUpdateCount > 0) {
                String message = String.format(ERROR_MESSAGE_TEMPLATE, statementsCount - failedUpdateCount, statementsCount);
                log.error(message);
                throw new RankingUpdateException(message);
            }
        }
    }

    private boolean isStatementResultKO(int value) {
        return value != 1 && value != Statement.SUCCESS_NO_INFO;
    }


    @Override
    public void updateRankingExt(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.updateRankingExt");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriod = {}", awardPeriod);
        }

        if (maxTransactionNumber == null) {
            throw new IllegalStateException("updateRankingExt must be called after updateRanking");
        }

        CitizenRankingExt rankingExt = CitizenRankingExt.builder()
                .awardPeriodId(awardPeriod.getAwardPeriodId())
                .minPosition(awardPeriod.getMinPosition())
                .maxPeriodCashback(awardPeriod.getMaxPeriodCashback())
                .totalParticipants(lastAssignedRanking.longValue())
                .minTransactionNumber(minTransactionNumber.longValue())
                .maxTransactionNumber(maxTransactionNumber.longValue())
                .updateDate(OffsetDateTime.now())
                .updateUser(RankingProcessorService.PROCESS_NAME)
                .build();

        int result = citizenRankingDao.updateRankingExt(rankingExt);

        if (isStatementResultKO(result)) {
            rankingExt.setInsertDate(rankingExt.getUpdateDate());
            rankingExt.setInsertUser(rankingExt.getUpdateUser());
            rankingExt.setUpdateDate(null);
            rankingExt.setUpdateUser(null);
            result = citizenRankingDao.insertRankingExt(rankingExt);

            if (isStatementResultKO(result)) {
                throw new RankingUpdateException("failed to update citizen_ranking_ext");
            }
        }
    }

}
