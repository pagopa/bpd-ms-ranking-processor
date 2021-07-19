package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

@Service
@Slf4j
class WinningTransactionDaoImpl implements WinningTransactionDao {

    private final String findPaymentTrxToProcessQuery;
    private final String findPartialTransferTrxToProcessQuery;
    private final String findTransferTrxToProcessQuery;
    private final String findPaymentTrxWithCorrelationIdQuery;
    private final String findPaymentTrxWithoutCorrelationIdQuery;
    private final String findProcessedTransferAmountQuery;
    private final String updateProcessedTrxSql;
    private final String updateUnrelatedTransferSql;
    private final String updateUnprocessedPartialTransferSql;
    private final String deleteTrxTransferSql;
    private final JdbcTemplate jdbcTemplate;
    private final RowMapper<WinningTransaction> paymentTrxRowMapper = new WinningTransactionMapper();
    private final RowMapperResultSetExtractor<WinningTransaction> paymentTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionMapper());
    private final RowMapperResultSetExtractor<WinningTransaction> transferTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionTotalTransferMapper());
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final boolean lockEnabled;


    @SneakyThrows
    @Autowired
    public WinningTransactionDaoImpl(@Qualifier("winningTransactionJdbcTemplate") JdbcTemplate jdbcTemplate,
                                     @Value("${winning-transaction.extraction-query.lock.enable}") boolean lockEnabled,
                                     @Value("${winning-transaction.extraction-query.elab-ranking.name}") String elabRankingName,
                                     @Value("${winning-transaction.extraction-query.transfer.table.name}") String transferTableName) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.WinningTransactionDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}, lockEnabled = {}, elabRankingName = {}", jdbcTemplate, lockEnabled, elabRankingName);
        }

        this.jdbcTemplate = jdbcTemplate;
        this.lockEnabled = lockEnabled;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);

        findPaymentTrxToProcessQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction where enabled_b is true and %s is not true and award_period_id_n = ? and operation_type_c != '01'",
                elabRankingName);
        updateProcessedTrxSql = String.format("update bpd_winning_transaction set %s = true, score_n = :score, update_date_t = :updateDate, update_user_s = :updateUser where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                elabRankingName);
        updateUnrelatedTransferSql = String.format("update %s set update_date_t = :updateDate, update_user_s = :updateUser, parked_b = :parked where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                transferTableName);
        updateUnprocessedPartialTransferSql = String.format("update %s set partial_transfer_b = true, update_date_t = :updateDate, update_user_s = :updateUser, parked_b = :parked where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                transferTableName);
        deleteTrxTransferSql = String.format("delete from %s where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                transferTableName);
        findPartialTransferTrxToProcessQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s, correlation_id_s, hpan_s, merchant_id_s, terminal_id_s, insert_date_t from %s transfer where transfer.award_period_id_n = ? and coalesce(transfer.update_date_t, '1900-01-01 00:00:00.000'::timestamptz) < ? and transfer.parked_b is not true",
                transferTableName);
        findTransferTrxToProcessQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s, correlation_id_s, hpan_s, merchant_id_s, terminal_id_s, insert_date_t from %s transfer where transfer.award_period_id_n = ? and coalesce(transfer.update_date_t, '1900-01-01 00:00:00.000'::timestamptz) < ? and transfer.partial_transfer_b is not true and transfer.parked_b is not true",
                transferTableName);
        findPaymentTrxWithCorrelationIdQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction payment where payment.enabled_b is true and payment.%s is true and payment.operation_type_c != '01' and payment.award_period_id_n = ? and payment.hpan_s = ? and payment.acquirer_c = ? and payment.acquirer_id_s = ? and payment.correlation_id_s = ?",
                elabRankingName);
        findPaymentTrxWithoutCorrelationIdQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction payment where payment.enabled_b is true and payment.%s is true and payment.operation_type_c != '01' and payment.award_period_id_n = ? and payment.hpan_s = ? and payment.acquirer_c = ? and payment.acquirer_id_s = ? and payment.amount_i = ? and payment.merchant_id_s = ? and payment.terminal_id_s = ?",
                elabRankingName);
        findProcessedTransferAmountQuery = String.format("select sum(amount_i) from bpd_winning_transaction where enabled_b is true and %s is true and operation_type_c = '01' and award_period_id_n = ? and hpan_s = ? and acquirer_c = ? and acquirer_id_s = ? and correlation_id_s = ?",
                elabRankingName);
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
        if (lockEnabled) {
            sql.append(" FOR UPDATE SKIP LOCKED");
        }
    }

    @Override
    public List<WinningTransaction> findPaymentToProcess(Long awardPeriodId, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findPaymentToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", awardPeriodId, pageable);
        }

        StringBuilder sql = new StringBuilder(findPaymentTrxToProcessQuery);
        managePagination(sql, pageable);
        manageLocking(sql);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                paymentTrxResultSetExtractor);
    }


    @Override
    public WinningTransaction findPaymentTrxWithCorrelationId(WinningTransaction.FilterCriteria filterCriteria) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findPaymentTrxWithCorrelationId");
        }
        if (log.isDebugEnabled()) {
            log.debug("filterCriteria = {}", filterCriteria);
        }

        WinningTransaction result;
        try {
            result = jdbcTemplate.queryForObject(findPaymentTrxWithCorrelationIdQuery,
                    paymentTrxRowMapper,
                    filterCriteria.getAwardPeriodId(),
                    filterCriteria.getHpan(),
                    filterCriteria.getAcquirerCode(),
                    filterCriteria.getAcquirerId(),
                    filterCriteria.getCorrelationId());

        } catch (EmptyResultDataAccessException e) {
            result = null;
        }

        return result;
    }


    @Override
    public WinningTransaction findPaymentTrxWithoutCorrelationId(WinningTransaction.FilterCriteria filterCriteria) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findPaymentTrxWithoutCorrelationId");
        }
        if (log.isDebugEnabled()) {
            log.debug("filterCriteria = {}", filterCriteria);
        }

        List<WinningTransaction> result = jdbcTemplate.query(connection -> connection.prepareStatement(findPaymentTrxWithoutCorrelationIdQuery),
                preparedStatement -> {
                    preparedStatement.setLong(1, filterCriteria.getAwardPeriodId());
                    preparedStatement.setString(2, filterCriteria.getHpan());
                    preparedStatement.setString(3, filterCriteria.getAcquirerCode());
                    preparedStatement.setString(4, filterCriteria.getAcquirerId());
                    preparedStatement.setBigDecimal(5, filterCriteria.getAmount());
                    preparedStatement.setString(6, filterCriteria.getMerchantId());
                    preparedStatement.setString(7, filterCriteria.getTerminalId());
                },
                paymentTrxResultSetExtractor);

        return result != null && result.size() > 0
                ? result.get(0)
                : null;
    }


    @Override
    public List<WinningTransaction> findTransferToProcess(WinningTransaction.FilterCriteria filterCriteria, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findTransferToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", filterCriteria, pageable);
        }

        StringBuilder sql = new StringBuilder(findTransferTrxToProcessQuery);
        managePagination(sql, pageable);
        manageLocking(sql);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> {
                    preparedStatement.setLong(1, filterCriteria.getAwardPeriodId());
                    preparedStatement.setTimestamp(2, new Timestamp(filterCriteria.getUpdateDate().toInstant().toEpochMilli()));
                },
                transferTrxResultSetExtractor);
    }


    @Override
    public List<WinningTransaction> findPartialTransferToProcess(WinningTransaction.FilterCriteria filterCriteria, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findPartialTransferToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("filterCriteria = {}, pageable = {}", filterCriteria, pageable);
        }

        StringBuilder sql = new StringBuilder(findPartialTransferTrxToProcessQuery);
        managePagination(sql, pageable);
        manageLocking(sql);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> {
                    preparedStatement.setLong(1, filterCriteria.getAwardPeriodId());
                    preparedStatement.setTimestamp(2, new Timestamp(filterCriteria.getUpdateDate().toInstant().toEpochMilli()));
                },
                transferTrxResultSetExtractor);
    }


    @Override
    public BigDecimal findProcessedTransferAmount(WinningTransaction.FilterCriteria filterCriteria) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findProcessedTransferAmount");
        }
        if (log.isDebugEnabled()) {
            log.debug("filterCriteria = {}", filterCriteria);
        }

        BigDecimal result;
        try {
            result = jdbcTemplate.queryForObject(findProcessedTransferAmountQuery,
                    BigDecimal.class,
                    filterCriteria.getAwardPeriodId(),
                    filterCriteria.getHpan(),
                    filterCriteria.getAcquirerCode(),
                    filterCriteria.getAcquirerId(),
                    filterCriteria.getCorrelationId());

        } catch (EmptyResultDataAccessException e) {
            result = null;
        }

        return result;
    }


    @Override
    public int[] deleteTransfer(List<WinningTransaction> winningTransactions) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.deleteTransfer");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactions = {}", winningTransactions);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactions);
        return namedParameterJdbcTemplate.batchUpdate(deleteTrxTransferSql, batchValues);
    }


    @Override
    public int[] updateProcessedTransaction(final Collection<WinningTransaction> winningTransactionIds) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateProcessedTransaction");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionIds = {}", winningTransactionIds);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactionIds);
        return namedParameterJdbcTemplate.batchUpdate(updateProcessedTrxSql, batchValues);
    }


    @Override
    public int[] updateUnrelatedTransfer(final Collection<WinningTransaction> winningTransactions) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateUnrelatedTransfer");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactions = {}", winningTransactions);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactions);
        return namedParameterJdbcTemplate.batchUpdate(updateUnrelatedTransferSql, batchValues);
    }


    @Override
    public int[] updateUnprocessedPartialTransfer(final Collection<WinningTransaction> winningTransactions) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateUnprocessedPartialTransfer");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactions = {}", winningTransactions);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactions);
        return namedParameterJdbcTemplate.batchUpdate(updateUnprocessedPartialTransferSql, batchValues);
    }


    @Slf4j
    static class WinningTransactionMapper implements RowMapper<WinningTransaction> {

        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            return WinningTransaction.builder()
                    .idTrxAcquirer(rs.getString("id_trx_acquirer_s"))
                    .acquirerCode(rs.getString("acquirer_c"))
                    .trxDate(rs.getObject("trx_timestamp_t", OffsetDateTime.class))
                    .operationType(rs.getString("operation_type_c"))
                    .acquirerId(rs.getString("acquirer_id_s"))
                    .fiscalCode(rs.getString("fiscal_code_s"))
                    .amount(rs.getBigDecimal("amount_i"))
                    .score(rs.getBigDecimal("score_n"))
                    .build();
        }
    }

    @Slf4j
    static class WinningTransactionTotalTransferMapper extends WinningTransactionMapper {

        @Override
        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            WinningTransaction winningTransaction = super.mapRow(rs, rowNum);
            winningTransaction.setCorrelationId(rs.getString("correlation_id_s"));
            winningTransaction.setHpan(rs.getString("hpan_s"));
            winningTransaction.setMerchantId(rs.getString("merchant_id_s"));
            winningTransaction.setTerminalId(rs.getString("terminal_id_s"));
            winningTransaction.setInsertDate(rs.getObject("insert_date_t", OffsetDateTime.class));
            return winningTransaction;
        }
    }

}
