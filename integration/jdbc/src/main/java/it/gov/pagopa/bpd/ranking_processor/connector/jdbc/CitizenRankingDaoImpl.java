package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
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

    public static final String FIND_ALL_ORDERED_BY_TRX_NUM_SQL = "select" +
            " *" +
            " from" +
            " bpd_citizen.bpd_citizen_ranking" +
            " where" +
            " award_period_id_n = ?";

    private static final String UPDATE_RANKING_SQL = "update" +
            " bpd_citizen.bpd_citizen_ranking" +
            " set" +
            " ranking_n = :ranking" +
            " where" +
            " fiscal_code_c = :fiscalCode" +
            " and award_period_id_n = :awardPeriodId";

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final RowMapperResultSetExtractor<CitizenRanking> findallResultSetExtractor;


    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate jdbcTemplate) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.CitizenRankingDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}", jdbcTemplate);
        }
        this.jdbcTemplate = jdbcTemplate;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        findallResultSetExtractor = new RowMapperResultSetExtractor<>(new CitizenRankingMapper());
    }


    @Override
    public int[] updateCashback(final Collection<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }
        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(citizenRankings.toArray());
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_CASHBACK_SQL, batchValues);
    }


    @Override
    public List<CitizenRanking> findAll(Long awardPeriodId, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.findAll");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", awardPeriodId, pageable);
        }
        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }
        StringBuilder sql = new StringBuilder(FIND_ALL_ORDERED_BY_TRX_NUM_SQL);
        if (pageable != null) {
            if (!pageable.getSort().isEmpty()) {
                sql.append(" ").append(pageable.getSort().toString().replace(":", ""));
            }
            sql.append(" LIMIT ").append(pageable.getPageSize())
                    .append(" OFFSET ").append(pageable.getOffset());
        }

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                findallResultSetExtractor);
    }


    @Override
    public int[] updateRanking(Collection<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }
        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(citizenRankings.toArray());
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_RANKING_SQL, batchValues);
    }


    @Slf4j
    static final class CitizenRankingMapper implements RowMapper<CitizenRanking> {

        public CitizenRanking mapRow(ResultSet rs, int rowNum) throws SQLException {
            if (log.isTraceEnabled()) {
                log.trace("CitizenRankingMapper.mapRow");
            }
            if (log.isDebugEnabled()) {
                log.debug("rs = {}, rowNum = {}", rs, rowNum);
            }
            return CitizenRanking.builder()
                    .fiscalCode(rs.getString("fiscal_code_c"))
                    .awardPeriodId(rs.getLong("award_period_id_n"))
                    .totalCashback(rs.getBigDecimal("cashback_n"))
                    .transactionNumber(rs.getLong("transaction_n"))
                    .ranking(rs.getLong("ranking_n"))
                    .rankingMinRequired(rs.getLong("ranking_min_n"))
                    .maxTotalCashback(rs.getBigDecimal("max_cashback_n"))
                    .build();
        }
    }

}
