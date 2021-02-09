package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import org.springframework.data.domain.Pageable;

import java.util.Collection;
import java.util.List;

public interface CitizenRankingDao {

    int[] updateCashback(Collection<CitizenRanking> citizenRankings);

    List<CitizenRanking> findAll(Long awardPeriodId, Pageable pageable);

    int[] updateRanking(Collection<CitizenRanking> citizenRankings);

}
