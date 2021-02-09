package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
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
class WinningWinningTransactionDaoImpl implements WinningTransactionDao {

    private static final String FIND_PAYMENT_TRX_TO_PROCESS_QUERY = "select" +
            " id_trx_acquirer_s," +
            " trx_timestamp_t," +
            " acquirer_c," +
            " acquirer_id_s," +
            " operation_type_c," +
            " score_n," +
            " amount_i," +
            " fiscal_code_s" +
            " from" +
            " bpd_winning_transaction.bpd_winning_transaction" +
            " where" +
            " enabled_b is true" +
            " and elab_ranking_b is false" +
            " and award_period_id_n = ?" +
            " and operation_type_c = '01'";

    private static final String FIND_TOTAL_TRANSFER_TRX_TO_PROCESS_QUERY = "select" +
            " id_trx_acquirer_s," +
            " trx_timestamp_t," +
            " acquirer_c," +
            " acquirer_id_s," +
            " operation_type_c," +
            " score_n," +
            " amount_i," +
            " fiscal_code_s" +
            " from" +
            " bpd_winning_transaction.bpd_winning_transaction transfer" +
            " where" +
            " transfer.enabled_b is true" +
            " and transfer.elab_ranking_b is false" +
            " and transfer.award_period_id_n = ?" +
            " and transfer.operation_type_c = '01'" +
            " and exists (" +
            " select" +
            " 1" +
            " from" +
            " bpd_winning_transaction.bpd_winning_transaction payment" +
            " where" +
            " payment.enabled_b is true" +
            " and payment.operation_type_c != transfer.operation_type_c" +
            " and payment.award_period_id_n = transfer.award_period_id_n" +
            " and payment.hpan_s = transfer.hpan_s" +
            " and payment.acquirer_c = transfer.acquirer_c" +
            " and payment.acquirer_id_s = transfer.acquirer_id_s" +
            " and payment.amount_i = transfer.amount_i" +
            " and ((nullif(transfer.correlation_id_s, '') is null" +
            " and payment.merchant_id_s = transfer.merchant_id_s" +
            " and payment.terminal_id_s = transfer.terminal_id_s)" +
            " or (nullif(transfer.correlation_id_s, '') is not null" +
            " and payment.correlation_id_s = transfer.correlation_id_s)))";

    private static final String FIND_PARTIAL_TRANSFER_TRX_TO_PROCESS_QUERY = ""; //TODO: insert SQL

    private static final String UPDATE_PROCESSED_TRX_SQL = "update" +
            " bpd_winning_transaction.bpd_winning_transaction" +
            " set" +
            " elab_ranking_b = true" +
            " where" +
            " id_trx_acquirer_s = :idTrxAcquirer" +
            " and wt.acquirer_c = :acquirerCode" +
            " and wt.trx_timestamp_t = :trxDate" +
            " and wt.operation_type_c = :operationType" +
            " and wt.acquirer_id_s = :acquirerId";

    private final JdbcTemplate jdbcTemplate;
    private final RowMapperResultSetExtractor<WinningTransaction> findTrxToProcessResultSetExtractor;

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final boolean lockEnabled;


    @Autowired
    public WinningWinningTransactionDaoImpl(@Qualifier("transactionJdbcTemplate") JdbcTemplate jdbcTemplate,
                                            @Value("${winning-transaction.extraction-query.lock.enable}") boolean lockEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("WinningWinningTransactionDaoImpl.WinningWinningTransactionDaoImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("jdbcTemplate = {}, lockEnabled = {}", jdbcTemplate, lockEnabled);
        }
        this.jdbcTemplate = jdbcTemplate;
        this.lockEnabled = lockEnabled;
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        findTrxToProcessResultSetExtractor = new RowMapperResultSetExtractor<>(new WinningTransactionMapper());
    }


    @Override
    public List<WinningTransaction> findTransactionToProcess(Long awardPeriodId,
                                                             TransactionType transactionType,
                                                             Pageable pageable) {
        if (log.isTraceEnabled()) {
            log.trace("WinningWinningTransactionDaoImpl.findTransactionToProcess");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, transactionType = {}, pageable = {}", awardPeriodId, transactionType, pageable);
        }

        StringBuilder sql = new StringBuilder();
        switch (transactionType) {
            case PAYMENT:
                sql.append(FIND_PAYMENT_TRX_TO_PROCESS_QUERY);
                break;
            case TOTAL_TRANSFER:
                sql.append(FIND_TOTAL_TRANSFER_TRX_TO_PROCESS_QUERY);
                break;
            case PARTIAL_TRANSFER:
                sql.append(FIND_PARTIAL_TRANSFER_TRX_TO_PROCESS_QUERY);
                break;
            default:
                throw new IllegalArgumentException(String.format("Transaction Type \"%s\" not allowed", transactionType));
        }

        if (pageable != null) {
            sql.append(" LIMIT ").append(pageable.getPageSize())
                    .append(" OFFSET ").append(pageable.getOffset());
        }

        if (lockEnabled) {
            sql.append(" FOR UPDATE SKIP LOCKED");
        }

        return jdbcTemplate.query(connection -> connection.prepareStatement(sql.toString()),
                preparedStatement -> preparedStatement.setLong(1, awardPeriodId),
                findTrxToProcessResultSetExtractor);
    }


    @Override
    public int[] updateProcessedTransaction(final Collection<WinningTransaction> winningTransactionIds) {
        if (log.isTraceEnabled()) {
            log.trace("WinningWinningTransactionDaoImpl.updateProcessedTransaction");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionIds = {}", winningTransactionIds);
        }
        SqlParameterSource[] batchValues = SqlParameterSourceUtils.createBatch(winningTransactionIds.toArray());
        return namedParameterJdbcTemplate.batchUpdate(UPDATE_PROCESSED_TRX_SQL, batchValues);
    }


    @Slf4j
    static final class WinningTransactionMapper implements RowMapper<WinningTransaction> {

        public WinningTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            if (log.isTraceEnabled()) {
                log.trace("WinningTransactionMapper.mapRow");
            }
            if (log.isDebugEnabled()) {
                log.debug("rs = {}, rowNum = {}", rs, rowNum);
            }
            return WinningTransaction.builder()
                    .idTrxAcquirer(rs.getString("id_trx_acquirer_s"))
                    .acquirerCode(rs.getString("acquirer_c"))
                    .trxDate(OffsetDateTime.parse(rs.getString("trx_timestamp_t")))
                    .operationType(rs.getString("operation_type_c"))
                    .acquirerId(rs.getString("acquirer_id_s"))
                    .fiscalCode(rs.getString("fiscal_code_s"))
                    .amount(rs.getBigDecimal("amount_i"))
                    .score(rs.getBigDecimal("score_n"))
                    .build();
        }
    }

}
