package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Collection;
import java.util.List;

public interface CitizenRankingDao {

    Sort SORT_BY_TRX_NUM_DESC = Sort.by(Sort.Direction.DESC, "transaction_n");

    int[] updateCashback(Collection<CitizenRanking> citizenRankings);

    List<CitizenRanking> findAll(Long awardPeriodId, Pageable pageable);

    List<CitizenRanking> findAll(Long awardPeriodId, Sort sort);

    int[] updateRanking(Collection<CitizenRanking> citizenRankings);

}
