package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import java.util.List;

public interface CitizenRankingDao {

    int[] updateCashback(List<Object> toUpdate);

}
