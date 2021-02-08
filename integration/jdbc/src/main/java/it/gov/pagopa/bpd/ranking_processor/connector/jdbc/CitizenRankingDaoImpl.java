package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
class CitizenRankingDaoImpl implements CitizenRankingDao {

    private static final String UPDATE_CASHBACK_SQL = "update" +
            " bpd_citizen.bpd_citizen_ranking" +
            " set" +
            " cashback_n = :cashback," +
            " transaction_n = :transactionCount," +
            " update_date_t = CURRENT_TIMESTAMP," +
            " update_user_s = 'RANKING-PROCESSOR'" +
            " where" +
            " fiscal_code_c = :fiscalCode" +
            " and award_period_id_n = :awardPeriodId";

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @Override
    public int[] updateCashback(final List<Object> toUpdate) {
        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(toUpdate.toArray());
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_CASHBACK_SQL, batchValues);
    }
}
