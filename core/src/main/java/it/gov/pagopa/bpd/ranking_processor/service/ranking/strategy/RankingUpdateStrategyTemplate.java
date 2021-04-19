package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import it.gov.pagopa.bpd.ranking_processor.service.ranking.RankingUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_RANKING_EXT;

/**
 * Template Method pattern of {@link RankingUpdateStrategy}
 */
@Slf4j
abstract class RankingUpdateStrategyTemplate implements RankingUpdateStrategy {

    static final String ERROR_MESSAGE_TEMPLATE = "updateRanking: affected %d rows of %d";
    protected static final Comparator<CitizenRanking> TIE_BREAK = Comparator.comparing(CitizenRanking::getFiscalCode);

    protected int lastAssignedRanking;
    protected final AtomicInteger minTransactionNumber = new AtomicInteger(Integer.MAX_VALUE);

    private Integer maxTransactionNumber;
    private final CitizenRankingDao citizenRankingDao;
    /**
     * A set of latest (in terms of ranking) ties. Required to manage ties between each chunks
     */
    private Set<CitizenRanking> lastTies = Collections.emptySet();
    private boolean updateRankingFailed;

    @Override
    @Transactional("citizenTransactionManager")
    public int process(AwardPeriod awardPeriod, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyTemplate.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, simplePageRequest = {}", awardPeriod, simplePageRequest);
        }

        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(),
                simplePageRequest.getSize(),
                CitizenRankingDao.FIND_ALL_PAGEABLE_SORT);
        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriod.getAwardPeriodId(), pageRequest);
        int totalExtractedRankings = citizenRankings.size();

        citizenRankings.addAll(lastTies);
        NavigableMap<Long, Set<CitizenRanking>> tiedMap = aggregateData(citizenRankings);
        setRanking(tiedMap, awardPeriod);

        if (lastAssignedRanking != pageRequest.getOffset() + totalExtractedRankings) {
            throw new IllegalStateException(String.format("Size of processed ranking records (%d) differs from the extracted one (%d)",
                    lastAssignedRanking,
                    pageRequest.getOffset() + totalExtractedRankings));
        }

        int[] affectedRows = citizenRankingDao.updateRanking(citizenRankings);

        try {
            checkErrors(citizenRankings.size(), affectedRows);

        } catch (RankingUpdateException e) {
            updateRankingFailed = true;
            throw e;
        }

        lastAssignedRanking -= lastTies.size();
        lastTies = tiedMap.lastEntry().getValue();

        if (maxTransactionNumber == null) {
            maxTransactionNumber = tiedMap.firstKey().intValue();
        }

        return totalExtractedRankings;
    }

    protected abstract void setRanking(Map<Long, Set<CitizenRanking>> tiedMap, AwardPeriod awardPeriod);


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
                    .filter(DaoHelper.isStatementResultKO)
                    .count();

            if (failedUpdateCount > 0) {
                String message = String.format(ERROR_MESSAGE_TEMPLATE, statementsCount - failedUpdateCount, statementsCount);
                log.error(message);
                throw new RankingUpdateException(message);
            }
        }
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
            log.info("skip {}", UPDATE_RANKING_EXT);

        } else {
            CitizenRankingExt rankingExt = CitizenRankingExt.builder()
                    .awardPeriodId(awardPeriod.getAwardPeriodId())
                    .minPosition(awardPeriod.getMinPosition())
                    .maxPeriodCashback(awardPeriod.getMaxPeriodCashback())
                    .totalParticipants(updateRankingFailed ? null : (long) lastAssignedRanking)
                    .minTransactionNumber(updateRankingFailed && lastAssignedRanking < awardPeriod.getMinPosition()
                            ? null
                            : minTransactionNumber.longValue())
                    .maxTransactionNumber(maxTransactionNumber.longValue())
                    .updateDate(OffsetDateTime.now())
                    .updateUser(RankingProcessorService.PROCESS_NAME)
                    .build();

            int result = citizenRankingDao.updateRankingExt(rankingExt);

            if (DaoHelper.isStatementResultKO.test(result)) {
                rankingExt.setInsertDate(rankingExt.getUpdateDate());
                rankingExt.setInsertUser(rankingExt.getUpdateUser());
                rankingExt.setUpdateDate(null);
                rankingExt.setUpdateUser(null);
                result = citizenRankingDao.insertRankingExt(rankingExt);

                if (DaoHelper.isStatementResultKO.test(result)) {
                    throw new RankingUpdateException("failed to update citizen_ranking_ext");
                }
            }
        }

    }

}
