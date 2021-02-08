package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
class CitizenRankingDaoImpl implements CitizenRankingDao {

    private final JdbcTemplate citizenJdbcTemplate;

    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate citizenJdbcTemplate) {
        this.citizenJdbcTemplate = citizenJdbcTemplate;
    }

}
