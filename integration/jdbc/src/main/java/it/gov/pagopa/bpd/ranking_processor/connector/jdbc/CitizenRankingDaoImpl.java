package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import lombok.AllArgsConstructor;
import lombok.Getter;
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
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Service
@Slf4j
class CitizenRankingDaoImpl implements CitizenRankingDao {

    private static final String UPDATE_REDIS_SQL = "UPDATE redis_cache_config SET update_ranking=true, update_ranking_from=CURRENT_TIMESTAMP";
    private static final String UPDATE_CASHBACK_SQL_TEMPLATE = "update %s bcr set cashback_n = cashback_n + :totalCashback, transaction_n = transaction_n + :transactionNumber, update_date_t = :updateDate, update_user_s = :updateUser where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId and exists (select 1 from bpd_citizen bc where bc.fiscal_code_s = bcr.fiscal_code_c and bc.enabled_b is true)";
    private static final String FINDALL_BY_AWARDPERIOD_AND_UPDATEDATE_SQL_TEMPLATE = "select bcr.fiscal_code_c, bcr.award_period_id_n, bcr.transaction_n, bcr.cashback_n, bcr.ranking_n from %s bcr inner join bpd_citizen.bpd_citizen bc on bc.fiscal_code_s = bcr.fiscal_code_c and bc.enabled_b is true where bcr.award_period_id_n = ? and coalesce(bcr.update_date_t,'1900-01-01 00:00:00.000'::timestamptz) < ?";
    private static final String UPDATE_RANKING_SQL_TEMPLATE = "update %s bcr set ranking_n = :ranking, update_date_t = :updateDate, update_user_s = :updateUser where fiscal_code_c = :fiscalCode and award_period_id_n = :awardPeriodId";
    private static final String UPDATE_RANKING_EXT_SQL_TEMPLATE = "update %s set ${TOTAL_PARTECIPANTS} ${MIN_TRANSACTION} max_transaction_n = :maxTransactionNumber, ranking_min_n = :minPosition, period_cashback_max_n = :maxPeriodCashback, update_date_t = :updateDate, update_user_s = :updateUser where award_period_id_n = :awardPeriodId";
    private static final String UPDATE_MILESTONE_SQL_TEMPLATE = "SELECT * from %s(?, ?, ?)";
    private static final String UPDATE_RANKING_PROCESSOR_LOCK_SQL = "update bpd_citizen.%s set worker_count = worker_count + :value, status = case when (worker_count + :value) = 0 then 'IDLE' else 'IN_PROGRESS' end, update_user = :updateUser, update_date = CURRENT_TIMESTAMP where process_id = :processId";
    private static final String GET_WORKER_COUNT_SQL = "select worker_count from bpd_ranking_processor_lock where process_id = ?";
    private static final String RESET_CASHBACK_SQL_TEMPLATE = "update %s bcr set cashback_n = 0, transaction_n = 0, update_date_t = :updateDate, update_user_s = :updateUser where fiscal_code_c = ? and award_period_id_n = ? and exists (select 1 from bpd_citizen bc where bc.fiscal_code_s = bcr.fiscal_code_c and bc.enabled_b is true)";

    private final String updateCashbackSql;
    private final String updateRankingSql;
    private final String updateRankingExtSql;
    private final String updateRankingProcessorLockSql;
    private final String findAllByAwardPeriodAndUpdateDateSql;
    private final String updateMilestoneSql;
    private final String resetCashbackSql;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final RowMapperResultSetExtractor<CitizenRanking> findAllResultSetExtractor = new RowMapperResultSetExtractor<>(new CitizenRankingMapper());
    private final SimpleJdbcInsertOperations insertRankingOps;
    private final SimpleJdbcInsertOperations insertRankingExtOps;


    @Autowired
    public CitizenRankingDaoImpl(@Qualifier("citizenJdbcTemplate") JdbcTemplate jdbcTemplate,
                                 @Value("${citizen.dao.table.name.ranking}") String rankingTableName,
                                 @Value("${citizen.dao.table.name.rankingExt}") String rankingExtTableName,
                                 @Value("${citizen.dao.table.name.rankingLock}") String rankingLockTableName,
                                 @Value("${citizen.dao.function.name.milestone}") String milestoneFunctionName) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.CitizenRankingDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}, rankingTableName = {}, rankingExtTableName = {}", jdbcTemplate, rankingTableName, rankingExtTableName);
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

        updateCashbackSql = String.format(UPDATE_CASHBACK_SQL_TEMPLATE,
                rankingTableName);
        findAllByAwardPeriodAndUpdateDateSql = String.format(FINDALL_BY_AWARDPERIOD_AND_UPDATEDATE_SQL_TEMPLATE,
                rankingTableName);
        updateRankingSql = String.format(UPDATE_RANKING_SQL_TEMPLATE,
                rankingTableName);
        updateRankingExtSql = String.format(UPDATE_RANKING_EXT_SQL_TEMPLATE,
                rankingExtTableName);
        updateRankingProcessorLockSql = String.format(UPDATE_RANKING_PROCESSOR_LOCK_SQL,
                rankingLockTableName);
        updateMilestoneSql = String.format(UPDATE_MILESTONE_SQL_TEMPLATE,
                milestoneFunctionName);
        resetCashbackSql = String.format(RESET_CASHBACK_SQL_TEMPLATE,
                rankingTableName);
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
    public int updateRedis() {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateRedis");
        }

        return jdbcTemplate.update(UPDATE_REDIS_SQL);
    }


    @Override
    public int updateMilestone(Integer offset, Integer limit, OffsetDateTime timestamp) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateMilestone");
        }

        return jdbcTemplate.queryForObject(updateMilestoneSql, Integer.class, offset, limit, timestamp);
    }


    @Override
    public int updateRankingExt(CitizenRankingExt rankingExt) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateRankingExt");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingExt = {}", rankingExt);
        }

        String query = updateRankingExtSql.replace("${TOTAL_PARTECIPANTS}",
                rankingExt.getTotalParticipants() == null
                        ? ""
                        : "total_participants = :totalParticipants,");
        query = query.replace("${MIN_TRANSACTION}",
                rankingExt.getMinTransactionNumber() == null
                        ? ""
                        : "min_transaction_n = :minTransactionNumber,");

        SqlParameterSource parameters = new BeanPropertySqlParameterSource(rankingExt);
        return namedParameterJdbcTemplate.update(query, parameters);
    }

    @Override
    public List<CitizenRanking> findAll(CitizenRanking.FilterCriteria filterCriteria, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.findAll");
        }
        if (log.isDebugEnabled()) {
            log.debug("filterCriteria = {}, pageable = {}", filterCriteria, pageable);
        }

        StringBuilder sql = new StringBuilder(findAllByAwardPeriodAndUpdateDateSql);
        managePagination(sql, pageable);
        manageLocking(sql);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> {
                    preparedStatement.setLong(1, filterCriteria.getAwardPeriodId());
                    preparedStatement.setTimestamp(2, new Timestamp(filterCriteria.getUpdateDate().toInstant().toEpochMilli()));
                },
                findAllResultSetExtractor);
    }

    private void managePagination(StringBuilder sql, Pageable pageable) {
        if (pageable != null) {
            if (!pageable.getSort().isEmpty()) {
                sql.append(" ORDER BY ").append(pageable.getSort().toString().replace(":", ""));
            }
            if (pageable.isPaged()) {
                sql.append(" LIMIT ").append(pageable.getPageSize())
                        .append(" OFFSET ").append(pageable.getOffset());
            }
        }
    }


    private void manageLocking(StringBuilder sql) {
        sql.append(" FOR UPDATE");
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

    @Override
    public int[] updateRanking(Collection<CitizenRanking> citizenRankings) {
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.updateRanking");
        }
        if (log.isDebugEnabled()) {
            log.debug("citizenRankings = {}", citizenRankings);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(citizenRankings);
        return namedParameterJdbcTemplate.batchUpdate(updateRankingSql, batchValues);
    }

    @Override
    public int registerWorker(RankingProcess process, boolean exclusiveLock) {
        return updateWorker(process, 1, exclusiveLock);
    }

    private int updateWorker(RankingProcess process, int value, boolean exclusiveLock) {
        String updateUser = System.getenv("HOSTNAME");
        if (updateUser == null) {
            updateUser = System.getenv("COMPUTERNAME");
        }
        UpdateWorkerDto updateWorkerDto = new UpdateWorkerDto(process.name(), value, updateUser);
        BeanPropertySqlParameterSource sqlParameterSource = new BeanPropertySqlParameterSource(updateWorkerDto);

        return namedParameterJdbcTemplate.update(exclusiveLock
                        ? updateRankingProcessorLockSql + " and worker_count = 0"
                        : updateRankingProcessorLockSql,
                sqlParameterSource);
    }

    @Override
    public int unregisterWorker(RankingProcess process) {
        return updateWorker(process, -1, false);
    }

    @Override
    public int getWorkerCount(RankingProcess process) {
        return jdbcTemplate.queryForObject(GET_WORKER_COUNT_SQL, Integer.class, process.name());
    }

    @Getter
    @AllArgsConstructor
    private static class UpdateWorkerDto {
        private String processId;
        private Integer value;
        private String updateUser;
    }

    @Override
    public int resetCashback(String fiscalCode, Long awardPeriodId) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("CitizenRankingDaoImpl.resetCashback");
        }
        if (log.isDebugEnabled()) {
            log.debug("fiscalCode = {}, awardPeriodId = {}", fiscalCode, awardPeriodId);
        }

        return jdbcTemplate.update(resetCashbackSql, preparedStatement -> {
                preparedStatement.setString(1, fiscalCode);
                preparedStatement.setLong(2, awardPeriodId);
                });
    }
}
