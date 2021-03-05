package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

@Service
@Slf4j
class WinningTransactionDaoImpl implements WinningTransactionDao {

    private final String findPaymentTrxToProcessQuery;
    private final String findTotalTransferTrxToProcessQuery;
    private final String findPartialTransferTrxToProcessQuery;
    private final String updateProcessedTrxSql;
    private final JdbcTemplate jdbcTemplate;
    private final RowMapperResultSetExtractor<WinningTransaction> paymentTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionMapper());
    private final RowMapperResultSetExtractor<WinningTransaction> totalTransferTrxResultSetExtractor = paymentTrxResultSetExtractor;
    private final RowMapperResultSetExtractor<WinningTransaction> partialTransferTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new ExtWinningTransactionMapper());

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final boolean lockEnabled;


    @SneakyThrows
    @Autowired
    public WinningTransactionDaoImpl(@Qualifier("winningTransactionJdbcTemplate") JdbcTemplate jdbcTemplate,
                                     @Value("${winning-transaction.extraction-query.lock.enable}") boolean lockEnabled,
                                     @Value("${winning-transaction.extraction-query.elab-ranking.name}") String elabRankingName) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.WinningTransactionDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}, lockEnabled = {}, elabRankingName = {}", jdbcTemplate, lockEnabled, elabRankingName);
        }

        this.jdbcTemplate = jdbcTemplate;
        this.lockEnabled = lockEnabled;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);

        findPaymentTrxToProcessQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction where enabled_b is true and %s is not true and award_period_id_n = ? and operation_type_c != '01' and fiscal_code_s is not null",
                elabRankingName);
        findTotalTransferTrxToProcessQuery = String.format("select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction transfer where transfer.enabled_b is true and transfer.%s is not true and transfer.award_period_id_n = ? and transfer.operation_type_c = '01' and exists ( select 1 from bpd_winning_transaction payment where payment.enabled_b is true and payment.operation_type_c != transfer.operation_type_c and payment.award_period_id_n = transfer.award_period_id_n and payment.hpan_s = transfer.hpan_s and payment.acquirer_c = transfer.acquirer_c and payment.acquirer_id_s = transfer.acquirer_id_s and payment.amount_i = transfer.amount_i and ((nullif(transfer.correlation_id_s, '') is null and payment.merchant_id_s = transfer.merchant_id_s and payment.terminal_id_s = transfer.terminal_id_s) or (nullif(transfer.correlation_id_s, '') is not null and payment.correlation_id_s = transfer.correlation_id_s))) and fiscal_code_s is not null",
                elabRankingName);
        updateProcessedTrxSql = String.format("update bpd_winning_transaction set %s = true, update_date_t = :updateDate, update_user_s = :updateUser where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                elabRankingName);
        findPartialTransferTrxToProcessQuery = String.format("select payment.amount_i - coalesce(sum(partial_transfer_elabled.amount_i), 0) as amount_balance, partial_transfer_to_elab.id_trx_acquirer_s, partial_transfer_to_elab.trx_timestamp_t, partial_transfer_to_elab.acquirer_c, partial_transfer_to_elab.acquirer_id_s, partial_transfer_to_elab.operation_type_c, partial_transfer_to_elab.score_n, partial_transfer_to_elab.amount_i, partial_transfer_to_elab.fiscal_code_s, partial_transfer_to_elab.correlation_id_s from bpd_winning_transaction.bpd_winning_transaction partial_transfer_to_elab inner join bpd_winning_transaction.bpd_winning_transaction payment on payment.enabled_b is true and payment.%s is true and payment.operation_type_c != partial_transfer_to_elab.operation_type_c and payment.award_period_id_n = partial_transfer_to_elab.award_period_id_n and payment.hpan_s = partial_transfer_to_elab.hpan_s and payment.acquirer_c = partial_transfer_to_elab.acquirer_c and payment.acquirer_id_s = partial_transfer_to_elab.acquirer_id_s and payment.amount_i != partial_transfer_to_elab.amount_i and payment.correlation_id_s = partial_transfer_to_elab.correlation_id_s left outer join bpd_winning_transaction.bpd_winning_transaction partial_transfer_elabled on partial_transfer_elabled.enabled_b is true and partial_transfer_elabled.%s is true and partial_transfer_elabled.operation_type_c = partial_transfer_to_elab.operation_type_c and partial_transfer_elabled.award_period_id_n = partial_transfer_to_elab.award_period_id_n and partial_transfer_elabled.hpan_s = partial_transfer_to_elab.hpan_s and partial_transfer_elabled.acquirer_c = partial_transfer_to_elab.acquirer_c and partial_transfer_elabled.acquirer_id_s = partial_transfer_to_elab.acquirer_id_s and partial_transfer_elabled.correlation_id_s = partial_transfer_to_elab.correlation_id_s where partial_transfer_to_elab.enabled_b is true and partial_transfer_to_elab.%s is not true and partial_transfer_to_elab.award_period_id_n = ? and partial_transfer_to_elab.operation_type_c = '01' and nullif(partial_transfer_to_elab.correlation_id_s, '') is not null and partial_transfer_to_elab.fiscal_code_s is not null group by partial_transfer_to_elab.id_trx_acquirer_s,  partial_transfer_to_elab.trx_timestamp_t,  partial_transfer_to_elab.acquirer_c,  partial_transfer_to_elab.acquirer_id_s,  partial_transfer_to_elab.operation_type_c, partial_transfer_to_elab.amount_i, partial_transfer_to_elab.fiscal_code_s, payment.amount_i",
                elabRankingName,
                elabRankingName,
                elabRankingName);
    }


    private void managePagination(StringBuilder sql, Pageable pageable) {
        if (pageable != null) {
            sql.append(" LIMIT ").append(pageable.getPageSize())
                    .append(" OFFSET ").append(pageable.getOffset());
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
    public List<WinningTransaction> findTotalTransferToProcess(Long awardPeriodId, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findTotalTransferToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", awardPeriodId, pageable);
        }

        StringBuilder sql = new StringBuilder(findTotalTransferTrxToProcessQuery);
        managePagination(sql, pageable);
        manageLocking(sql);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                totalTransferTrxResultSetExtractor);
    }


    @Override
    public List<WinningTransaction> findPartialTranferToProcess(Long awardPeriodId, Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findPartialTranferToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, pageable = {}", awardPeriodId, pageable);
        }

        StringBuilder sql = new StringBuilder(findPartialTransferTrxToProcessQuery);
        managePagination(sql, pageable);

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                partialTransferTrxResultSetExtractor);
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
    static class ExtWinningTransactionMapper extends WinningTransactionMapper {

        @Override
        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            WinningTransaction winningTransaction = super.mapRow(rs, rowNum);
            winningTransaction.setAmountBalance(rs.getBigDecimal("amount_balance"));
            winningTransaction.setCorrelationId(rs.getString("correlation_id_s"));
            return winningTransaction;
        }
    }

}
