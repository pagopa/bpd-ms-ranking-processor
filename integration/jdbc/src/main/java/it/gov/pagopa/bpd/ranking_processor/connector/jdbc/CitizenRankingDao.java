package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Collection;
import java.util.List;

/**
 * Data Access Object to manage the operations to the database related to {@link CitizenRanking} model
 */
public interface CitizenRankingDao {

    /**
     * Sort by transaction number with reverse order
     */
    Sort SORT_BY_TRX_NUM_DESC = Sort.by(Sort.Direction.DESC, "transaction_n");

    int[] updateCashback(List<CitizenRanking> citizenRankings);

    int[] insertCashback(List<CitizenRanking> citizenRankings);

    List<CitizenRanking> findAll(Long awardPeriodId, Pageable pageable);

    List<CitizenRanking> findAll(Long awardPeriodId, Sort sort);

    int[] updateRanking(Collection<CitizenRanking> citizenRankings);

}
