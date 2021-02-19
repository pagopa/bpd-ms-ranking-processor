package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcInsertOperations;
import org.springframework.stereotype.Service;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

@Service
@Slf4j
class CitizenRankingDaoImpl implements CitizenRankingDao {

    private static final String USER_VALUE = "RANKING-PROCESSOR";

    public final String findAllOrderedByTrxNumSql;
    private final String updateCashbackSql;
    private final String updateRankingSql;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final RowMapperResultSetExtractor<CitizenRanking> findallResultSetExtractor = new RowMapperResultSetExtractor<>(new CitizenRankingMapper());
    private final SimpleJdbcInsertOperations simpleJdbcInsertOps;


    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate jdbcTemplate,
                                 @Value("${citizen.dao.table.name}") String tableName) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.CitizenRankingDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}", jdbcTemplate);
        }

        this.jdbcTemplate = jdbcTemplate;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        simpleJdbcInsertOps = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName(tableName)
                .usingColumns("fiscal_code_c", "award_period_id_n", "transaction_n", "cashback_n", "insert_date_t", "insert_user_s");
        updateCashbackSql = String.format("update %s set cashback_n = cashback_n + :totalCashback, transaction_n = transaction_n + :transactionNumber, update_date_t = CURRENT_TIMESTAMP, update_user_s = '%s' where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId", tableName, USER_VALUE);
        findAllOrderedByTrxNumSql = String.format("select * from %s where award_period_id_n = ?", tableName);
        updateRankingSql = String.format("update %s set ranking_n = :ranking where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId", tableName);
    }


    @Override
    public int[] updateCashback(final List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }
        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(citizenRankings);
        return namedParameterJdbcTemplate.batchUpdate(updateCashbackSql, batchValues);
    }

    public int[] insertCashback(final List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        SqlParameterSource[] batchValues = new SqlParameterSource[citizenRankings.size()];
        OffsetDateTime now = OffsetDateTime.now();
        for (int i = 0; i < citizenRankings.size(); i++) {
            SqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("fiscal_code_c", citizenRankings.get(i).getFiscalCode())
                    .addValue("award_period_id_n", citizenRankings.get(i).getAwardPeriodId())
                    .addValue("transaction_n", citizenRankings.get(i).getTransactionNumber())
                    .addValue("cashback_n", citizenRankings.get(i).getTotalCashback().doubleValue())
                    .addValue("insert_date_t", now, JDBCType.TIMESTAMP_WITH_TIMEZONE.getVendorTypeNumber())
                    .addValue("insert_user_s", USER_VALUE);
            batchValues[i] = parameters;
        }

        return simpleJdbcInsertOps.executeBatch(batchValues);
    }


    @Override
    public List<CitizenRanking> findAll(long awardPeriodId, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.findAll");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", awardPeriodId, pageable);
        }

        StringBuilder clauses = new StringBuilder();
        if (pageable != null) {
            if (!pageable.getSort().isEmpty()) {
                clauses.append(" ORDER BY ").append(pageable.getSort().toString().replace(":", ""));
            }
            if (pageable.isPaged()) {
                clauses.append(" LIMIT ").append(pageable.getPageSize())
                        .append(" OFFSET ").append(pageable.getOffset());
            }
        }

        return findAll(awardPeriodId, clauses.toString());
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
        return namedParameterJdbcTemplate.batchUpdate(updateRankingSql, batchValues);
    }


    private List<CitizenRanking> findAll(Long awardPeriodId, String clauses) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.findAll");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, clauses = {}", awardPeriodId, clauses);
        }
        log.info("findAllOrderedByTrxNumSql + clauses = {}", findAllOrderedByTrxNumSql + clauses);

        return jdbcTemplate.query(connection -> connection.prepareStatement(findAllOrderedByTrxNumSql + clauses),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                findallResultSetExtractor);
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
//                    .rankingMinRequired(rs.getLong("ranking_min_n"))
//                    .maxTotalCashback(rs.getBigDecimal("max_cashback_n"))
                    .build();
        }
    }

}
