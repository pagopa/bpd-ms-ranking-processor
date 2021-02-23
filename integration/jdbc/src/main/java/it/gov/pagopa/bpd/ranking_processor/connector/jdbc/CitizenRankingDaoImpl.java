package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.*;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcInsertOperations;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Service
@Slf4j
class CitizenRankingDaoImpl implements CitizenRankingDao {

    public final String findAllOrderedByTrxNumSql;
    private final String updateCashbackSql;
    private final String updateRankingSql;
    private final String updateRankingExtSql;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final RowMapperResultSetExtractor<CitizenRanking> findallResultSetExtractor = new RowMapperResultSetExtractor<>(new CitizenRankingMapper());
    private final SimpleJdbcInsertOperations insertRankingOps;
    private final SimpleJdbcInsertOperations insertRankingExtOps;


    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate jdbcTemplate,
                                 @Value("${citizen.dao.table.name.ranking}") String rankingTableName,
                                 @Value("${citizen.dao.table.name.rankingExt}") String rankingExtTableName) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.CitizenRankingDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}", jdbcTemplate);
        }

        this.jdbcTemplate = jdbcTemplate;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);

        String[] rankingColumns = Arrays.stream(CitizenRanking.class.getDeclaredFields())
                .map(field -> field.getAnnotation(Column.class))
                .map(Column::value)
                .toArray(String[]::new);
        insertRankingOps = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName(rankingTableName)
                .usingColumns(rankingColumns);

        String[] rankingExtColumns = Arrays.stream(CitizenRankingExt.class.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Column.class))
                .map(field -> field.getAnnotation(Column.class))
                .map(Column::value)
                .toArray(String[]::new);
        insertRankingExtOps = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName(rankingExtTableName)
                .usingColumns(rankingExtColumns);

        updateCashbackSql = String.format("update %s set cashback_n = cashback_n + :totalCashback, transaction_n = transaction_n + :transactionNumber, update_date_t = :updateDate, update_user_s = :updateUser where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId", rankingTableName);
        findAllOrderedByTrxNumSql = String.format("select fiscal_code_c, award_period_id_n, transaction_n, cashback_n, ranking_n from %s where award_period_id_n = ?", rankingTableName);
        updateRankingSql = String.format("update %s set ranking_n = :ranking, update_date_t = :updateDate, update_user_s = :updateUser where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId", rankingTableName);
        updateRankingExtSql = String.format("update %s set total_participants = :totalParticipants, min_transaction_n = :minTransactionNumber, max_transaction_n = :maxTransactionNumber, ranking_min_n = :minPosition, period_cashback_max_n = :maxPeriodCashback, update_date_t = :updateDate, update_user_s = :updateUser where award_period_id_n = :awardPeriodId", rankingExtTableName);
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

    @Override
    public int[] insertCashback(final List<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        SqlParameterSource[] batchValues = toSqlParameterSources(citizenRankings);
        return insertRankingOps.executeBatch(batchValues);
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
    public int updateRankingExt(CitizenRankingExt rankingExt) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateRankingExt");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingExt = {}", rankingExt);
        }

        SqlParameterSource parameters = new BeanPropertySqlParameterSource(rankingExt);
        return namedParameterJdbcTemplate.update(updateRankingExtSql, parameters);
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

    @Override
    public int insertRankingExt(CitizenRankingExt rankingExt) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.insertRankingExt");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingExt = {}", rankingExt);
        }

        SqlParameterSource parameters = toSqlParameterSource(rankingExt);
        return insertRankingExtOps.execute(parameters);
    }

    private static SqlParameterSource[] toSqlParameterSources(Collection<?> candidates) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.toSqlParameterSources");
        }
        if (log.isDebugEnabled()) {
            log.debug("candidates = {}", candidates);
        }

        SqlParameterSource[] batchValues = new SqlParameterSource[candidates.size()];
        int i = 0;

        for (Object candidate : candidates) {
            batchValues[i] = toSqlParameterSource(candidate);
            i++;
        }

        return batchValues;
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
                    .build();
        }
    }

    @SneakyThrows
    private static SqlParameterSource toSqlParameterSource(Object candidate) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.toSqlParameterSource");
        }
        if (log.isDebugEnabled()) {
            log.debug("candidate = {}", candidate);
        }

        MapSqlParameterSource param = new MapSqlParameterSource();

        for (Field field : candidate.getClass().getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);

            if (column != null) {
                field.setAccessible(true);
                if (OffsetDateTime.class.isAssignableFrom(field.getType())) {
                    param.addValue(column.value(), field.get(candidate), JDBCType.TIMESTAMP_WITH_TIMEZONE.getVendorTypeNumber());
                } else {
                    param.addValue(column.value(), field.get(candidate));
                }
            }
        }

        return param;
    }

    private List<CitizenRanking> findAll(Long awardPeriodId, String clauses) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.findAll");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, clauses = {}", awardPeriodId, clauses);
        }

        return jdbcTemplate.query(connection -> connection.prepareStatement(findAllOrderedByTrxNumSql + clauses),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                findallResultSetExtractor);
    }

}
