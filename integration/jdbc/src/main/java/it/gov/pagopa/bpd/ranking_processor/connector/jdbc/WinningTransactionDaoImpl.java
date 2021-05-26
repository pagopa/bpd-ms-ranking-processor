package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.ActiveUserWinningTransaction;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
class WinningTransactionDaoImpl implements WinningTransactionDao {

    public static final String UPDATE_UNRELATED_TRANSFER_SQL = "update bpd_winning_transaction_transfer set update_date_t = :updateDate, update_user_s = :updateUser, parked_b = :parked where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId";
    public static final String UPDATE_UNPROCESSED_PARTIAL_TRANSFER_SQL = "update bpd_winning_transaction_transfer set partial_transfer_b = true, update_date_t = :updateDate, update_user_s = :updateUser where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId";
    public static final String FIND_PAYMENT_TRX_WITH_CORRELATION_ID_QUERY_TEMPLATE = "select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction payment where payment.enabled_b is true and payment.%s is true and payment.operation_type_c != '01' and payment.award_period_id_n = ? and payment.hpan_s = ? and payment.acquirer_c = ? and payment.acquirer_id_s = ? and payment.correlation_id_s = ?";
    public static final String FIND_PAYMENT_TRX_WITHOUT_CORRELATION_ID_QUERY_TEMPLATE = "select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s from bpd_winning_transaction payment where payment.enabled_b is true and payment.%s is true and payment.operation_type_c != '01' and payment.award_period_id_n = ? and payment.hpan_s = ? and payment.acquirer_c = ? and payment.acquirer_id_s = ? and payment.amount_i = ? and payment.merchant_id_s = ? and payment.terminal_id_s = ?";
    public static final String FIND_TRANSFER_TRX_TO_PROCESS_QUERY_TEMPLATE = "select id_trx_acquirer_s, trx_timestamp_t, acquirer_c, acquirer_id_s, operation_type_c, score_n, amount_i, fiscal_code_s, correlation_id_s, hpan_s, merchant_id_s, terminal_id_s from bpd_winning_transaction_transfer transfer where transfer.award_period_id_n = ? and transfer.insert_date_t > current_timestamp - interval '%s' and coalesce(transfer.update_date_t, '1900-01-01 00:00:00.000'::timestamptz) < ? and transfer.partial_transfer_b is not true and transfer.parked_b is not true";
    public static final String FIND_ACTIVE_USERS_SINCE_LAST_DETECTOR = "select bwt.fiscal_code_s, bwt.merchant_id_s, date_trunc('day', trx_timestamp_t) as payment_day, max(insert_date_t) from bpd_winning_transaction.bpd_winning_transaction bwt where insert_date_t > ( select bct.last_execution from bpd_winning_transaction.bpd_check_transaction bct where bct.id = 'DAILY_PAYMENT_LIMIT' ) and bwt.award_period_id_n = ? and bwt.operation_type_c != '01' and valid_b is not false group by (fiscal_code_s, merchant_id_s, date_trunc('day', trx_timestamp_t))";
    public static final String FIND_TOP_VALID_WINNING_TRANSACTIONS = "select fiscal_code_s, id_trx_acquirer_s, acquirer_c, trx_timestamp_t, operation_type_c, acquirer_id_s from bpd_winning_transaction.bpd_winning_transaction bwt where bwt.award_period_id_n = ? and bwt.fiscal_code_s = ? and bwt.operation_type_c != '01' and valid_b is not false and merchant_id_s = ? and trx_timestamp_t between ? and ? order by amount_i desc limit ?+1";
    public static final String UPDATE_DETECTOR_SET_LAST_EXECUTION = "update bpd_winning_transaction.bpd_check_transaction set last_execution = ? where bct.id = 'DAILY_PAYMENT_LIMIT'";
    public static final String UPDATE_INVALIDATE_TRANSACTIONS = "update bpd_winning_transaction.bpd_winning_transaction set valid_b = false where fiscal_code_s = ? and trx_timestamp_t between ? and ? and merchant_id_s = ? and award_period_id_n = ?";
    public static final String UPDATE_VALIDATE_TOP_TRANSACTIONS = "update bpd_winning_transaction.bpd_winning_transaction set valid_b = true where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId and fiscal_code_s = :fiscalCode";
    public static final String UPDATE_USER_TRANSACTIONS_ELAB_FALSE = "update bpd_winning_transaction.bpd_winning_transaction set elab_ranking_b = false where fiscal_code_s = ? and valid_b = true and award_period_id_n = ?";

    private final String findPaymentTrxToProcessQuery;
    private final String findPartialTransferTrxToProcessQuery;
    private final String findTransferTrxToProcessQuery;
    private final String findPaymentTrxWithCorrelationIdQuery;
    private final String findPaymentTrxWithoutCorrelationIdQuery;
    private final String updateProcessedTrxSql;
    private final String deleteProcessedTrxTransferSql;
    private final JdbcTemplate jdbcTemplate;
    private final RowMapper<WinningTransaction> paymentTrxRowMapper = new WinningTransactionMapper();
    private final RowMapperResultSetExtractor<WinningTransaction> paymentTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionMapper());
    private final RowMapperResultSetExtractor<WinningTransaction> transferTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionTotalTransferMapper());
    private final RowMapperResultSetExtractor<WinningTransaction> partialTransferTrxResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionPartialTransferMapper());
    private final RowMapperResultSetExtractor<ActiveUserWinningTransaction> activeUsersResultSetExtractor = new RowMapperResultSetExtractor<>(new ActiveUsersMapper());
    private final RowMapperResultSetExtractor<WinningTransaction> validTransactionsResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionValidMapper());

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final boolean lockEnabled;


    @SneakyThrows
    @Autowired
    public WinningTransactionDaoImpl(@Qualifier("winningTransactionJdbcTemplate") JdbcTemplate jdbcTemplate,
                                     @Value("${winning-transaction.extraction-query.lock.enable}") boolean lockEnabled,
                                     @Value("${winning-transaction.extraction-query.elab-ranking.name}") String elabRankingName,
                                     @Value("${winning-transaction.extraction-query.transfer.max-depth}") String transferMaxDepth) {
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
        updateProcessedTrxSql = String.format("update bpd_winning_transaction set %s = true, score_n = :score, update_date_t = :updateDate, update_user_s = :updateUser, valid_b = :valid where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId",
                elabRankingName);
        deleteProcessedTrxTransferSql = "delete from bpd_winning_transaction_transfer where id_trx_acquirer_s = :idTrxAcquirer and acquirer_c = :acquirerCode and trx_timestamp_t = :trxDate and operation_type_c = :operationType and acquirer_id_s = :acquirerId";
        findPartialTransferTrxToProcessQuery = String.format("select payment.amount_i as original_amount_balance, payment.amount_i - coalesce(sum(partial_transfer_elabled.amount_i), 0) as amount_balance, partial_transfer_to_elab.id_trx_acquirer_s, partial_transfer_to_elab.trx_timestamp_t, partial_transfer_to_elab.acquirer_c, partial_transfer_to_elab.acquirer_id_s, partial_transfer_to_elab.operation_type_c, partial_transfer_to_elab.score_n, partial_transfer_to_elab.amount_i, partial_transfer_to_elab.fiscal_code_s, partial_transfer_to_elab.correlation_id_s from bpd_winning_transaction.bpd_winning_transaction_transfer partial_transfer_to_elab inner join bpd_winning_transaction.bpd_winning_transaction payment on payment.enabled_b is true and payment.%s is true and payment.operation_type_c != partial_transfer_to_elab.operation_type_c and payment.award_period_id_n = partial_transfer_to_elab.award_period_id_n and payment.hpan_s = partial_transfer_to_elab.hpan_s and payment.acquirer_c = partial_transfer_to_elab.acquirer_c and payment.acquirer_id_s = partial_transfer_to_elab.acquirer_id_s and payment.amount_i != partial_transfer_to_elab.amount_i and payment.correlation_id_s = partial_transfer_to_elab.correlation_id_s left outer join bpd_winning_transaction.bpd_winning_transaction partial_transfer_elabled on partial_transfer_elabled.enabled_b is true and partial_transfer_elabled.%s is true and partial_transfer_elabled.operation_type_c = partial_transfer_to_elab.operation_type_c and partial_transfer_elabled.award_period_id_n = partial_transfer_to_elab.award_period_id_n and partial_transfer_elabled.hpan_s = partial_transfer_to_elab.hpan_s and partial_transfer_elabled.acquirer_c = partial_transfer_to_elab.acquirer_c and partial_transfer_elabled.acquirer_id_s = partial_transfer_to_elab.acquirer_id_s and partial_transfer_elabled.correlation_id_s = partial_transfer_to_elab.correlation_id_s where partial_transfer_to_elab.enabled_b is true and partial_transfer_to_elab.%s is not true and partial_transfer_to_elab.award_period_id_n = ? and partial_transfer_to_elab.operation_type_c = '01' and nullif(partial_transfer_to_elab.correlation_id_s, '') is not null and partial_transfer_to_elab.insert_date_t > current_timestamp - interval '%s' group by partial_transfer_to_elab.id_trx_acquirer_s, partial_transfer_to_elab.trx_timestamp_t, partial_transfer_to_elab.acquirer_c, partial_transfer_to_elab.acquirer_id_s, partial_transfer_to_elab.operation_type_c, partial_transfer_to_elab.amount_i, partial_transfer_to_elab.fiscal_code_s, payment.amount_i order by partial_transfer_to_elab.trx_timestamp_t",
                elabRankingName,
                elabRankingName,
                elabRankingName,
                transferMaxDepth);
        findTransferTrxToProcessQuery = String.format(FIND_TRANSFER_TRX_TO_PROCESS_QUERY_TEMPLATE,
                transferMaxDepth);
        findPaymentTrxWithCorrelationIdQuery = String.format(FIND_PAYMENT_TRX_WITH_CORRELATION_ID_QUERY_TEMPLATE,
                elabRankingName);
        findPaymentTrxWithoutCorrelationIdQuery = String.format(FIND_PAYMENT_TRX_WITHOUT_CORRELATION_ID_QUERY_TEMPLATE,
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
        namedParameterJdbcTemplate.batchUpdate(deleteProcessedTrxTransferSql, batchValues);
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
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_UNRELATED_TRANSFER_SQL, batchValues);
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
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_UNPROCESSED_PARTIAL_TRANSFER_SQL, batchValues);
    }

    @Override
    public List<ActiveUserWinningTransaction> findActiveUsersSinceLastDetector(Long awardPeriodId) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findActiveUsersSinceLastDetector");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }


        return jdbcTemplate.query(connection -> connection.prepareStatement(FIND_ACTIVE_USERS_SINCE_LAST_DETECTOR),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                activeUsersResultSetExtractor);
    }

    @Override
    public List<WinningTransaction> findTopValidWinningTransactions(Long awardPeriodId, int validPayments, String fiscalCode, String merchant, LocalDate paymentDate) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.findTopValidWinningTransactions");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, validPayments = {}, fiscalCode = {}, merchant = {}, paymentDate = {}", awardPeriodId, validPayments, fiscalCode, merchant, paymentDate);
        }

        List<WinningTransaction> winningTransactionList = jdbcTemplate.query(FIND_TOP_VALID_WINNING_TRANSACTIONS,
                validTransactionsResultSetExtractor,
                    awardPeriodId,
                    fiscalCode,
                    merchant,
                    paymentDate,
                    paymentDate.plusDays(1),
                    validPayments);

        if(null==winningTransactionList || winningTransactionList.size()<=validPayments){
            return Collections.emptyList();
        }
        winningTransactionList.remove(validPayments);
        return winningTransactionList;
    }

    @Override
    public int updateDetectorLastExecution(OffsetDateTime maxInsertDate) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateDetectorLastExecution");
        }
        if (log.isDebugEnabled()) {
            log.debug("maxInsertDate = {}", maxInsertDate);
        }
        return jdbcTemplate.update(UPDATE_DETECTOR_SET_LAST_EXECUTION, preparedStatement -> preparedStatement.setObject(1, maxInsertDate));
    }

    @Override
    public int updateInvalidateTransactions(String fiscalCode, LocalDate paymentDate, String merchant, Long awardPeriodId) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateInvalidateTransactions");
        }
        if (log.isDebugEnabled()) {
            log.debug("fiscalCode = {}, paymentDate = {}, merchant = {}, awardPeriodId = {}", fiscalCode, paymentDate, merchant, awardPeriodId);
        }
        return jdbcTemplate.update(UPDATE_INVALIDATE_TRANSACTIONS,
                fiscalCode,
                paymentDate,
                paymentDate.plusDays(1),
                merchant,
                awardPeriodId);
    }

    @Override
    public int[] updateSetValidTransactions(List<WinningTransaction> winningTransactionList) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateSetValidTransactions");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionList = {}", winningTransactionList);
        }

        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactionList);
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_VALIDATE_TOP_TRANSACTIONS, batchValues);
    }

    @Override
    public int updateUserTransactionsElab(String fiscalCode, Long awardPeriodId) {//TODO da testare su SIT
        if (log.isTraceEnabled()) {
            log.trace("WinningTransactionDaoImpl.updateUserTransactionsElab");
        }
        if (log.isDebugEnabled()) {
            log.debug("fiscalCode = {}, awardPeriodId = {}", fiscalCode, awardPeriodId);
        }
        return jdbcTemplate.update(UPDATE_USER_TRANSACTIONS_ELAB_FALSE, preparedStatement ->{
                        preparedStatement.setString(1, fiscalCode);
                        preparedStatement.setLong(2, awardPeriodId);
                });
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
            return winningTransaction;
        }
    }

    @Slf4j
    static class WinningTransactionPartialTransferMapper extends WinningTransactionMapper {

        @Override
        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            WinningTransaction winningTransaction = super.mapRow(rs, rowNum);
            winningTransaction.setCorrelationId(rs.getString("correlation_id_s"));
            winningTransaction.setAmountBalance(rs.getBigDecimal("amount_balance"));
            winningTransaction.setOriginalAmountBalance(rs.getBigDecimal("original_amount_balance"));
            return winningTransaction;
        }
    }

    @Slf4j
    static class ActiveUsersMapper implements RowMapper<ActiveUserWinningTransaction> {

        @Override
        public ActiveUserWinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            return ActiveUserWinningTransaction.builder()
                    .fiscalCode(rs.getString("fiscal_code_s"))
                    .merchantId(rs.getString("merchant_id_s"))
                    .trxDate(rs.getObject("payment_day", OffsetDateTime.class).toLocalDate())
                    .insertDate(rs.getObject("insert_date_t",OffsetDateTime.class))
                    .build();
        }
    }

    @Slf4j
    static class WinningTransactionValidMapper implements RowMapper<WinningTransaction> {

        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            return WinningTransaction.builder()
                    .idTrxAcquirer(rs.getString("id_trx_acquirer_s"))
                    .acquirerCode(rs.getString("acquirer_c"))
                    .trxDate(rs.getObject("trx_timestamp_t", OffsetDateTime.class))
                    .operationType(rs.getString("operation_type_c"))
                    .acquirerId(rs.getString("acquirer_id_s"))
                    .fiscalCode(rs.getString("fiscal_code_s"))
                    .build();
        }
    }

}
