package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Collection;
import java.util.List;

/**
 * Data Access Object to manage the operations to the database related to {@link CitizenRanking} model
 */
public interface CitizenRankingDao {

    int updateRedis();

    /**
     * Sort to retrieve citizens sorted by their ranking
     */
    Sort FIND_ALL_PAGEABLE_SORT = Sort.by(Sort.Order.desc("transaction_n"), Sort.Order.asc("fiscal_code_c"));

    int[] updateCashback(List<CitizenRanking> citizenRankings);

    int[] insertCashback(List<CitizenRanking> citizenRankings);

    List<CitizenRanking> findAll(long awardPeriodId, Pageable pageable);

    int[] updateRanking(Collection<CitizenRanking> citizenRankings);

    int updateRankingExt(CitizenRankingExt rankingExt);

    int insertRankingExt(CitizenRankingExt rankingExt);

    int registerWorker(RankingProcess process, boolean exclusiveLock);

    int unregisterWorker(RankingProcess process);

    int getWorkerCount(RankingProcess process);

    enum RankingProcess {
        UPDATE_CASHBACK,
        UPDATE_CASHBACK_PAYMENT,
        UPDATE_CASHBACK_TOTAL_TRANSFER,
        UPDATE_CASHBACK_PARTIAL_TRANSFER,
        UPDATE_RANKING,
        UPDATE_REDIS;
    }
}
