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
    Sort FIND_ALL_PAGEABLE_SORT = Sort.by(Sort.Order.desc("transaction_n"), Sort.Order.asc("fiscal_code_c"));

    int[] updateCashback(List<CitizenRanking> citizenRankings);

    int[] insertCashback(List<CitizenRanking> citizenRankings);

    List<CitizenRanking> findAll(long awardPeriodId, Pageable pageable);

    int[] updateRanking(Collection<CitizenRanking> citizenRankings);

}
