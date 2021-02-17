package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import it.gov.pagopa.bpd.common.BaseTest;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class WinningTransactionDaoImplTest extends BaseTest {

    private final WinningTransactionDaoImpl winningWinningTransactionDao;
    private final JdbcTemplate jdbcTemplateMock;


    public WinningTransactionDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        winningWinningTransactionDao = new WinningTransactionDaoImpl(jdbcTemplateMock, true);
    }


    @Test
    public void findTransactionToProcessOK_withPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        for (TransactionType type : TransactionType.values()) {
            List<WinningTransaction> transactions = winningWinningTransactionDao.findTransactionToProcess(1L,
                    type,
                    PageRequest.of(0, 1));

            Assert.assertNotNull(transactions);
            Assert.assertEquals(0, transactions.size());
        }
    }


    @Test
    public void findTransactionToProcessOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        for (TransactionType type : TransactionType.values()) {
            List<WinningTransaction> transactions = winningWinningTransactionDao.findTransactionToProcess(1L,
                    type,
                    null);

            Assert.assertNotNull(transactions);
            Assert.assertEquals(0, transactions.size());
        }
    }


    @Test
    public void updateProcessedTransactionOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = winningWinningTransactionDao.updateProcessedTransaction(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }


    public static class WinningTransactionMapperTest {

        private final WinningTransactionDaoImpl.WinningTransactionMapper winningTransactionMapper;

        public WinningTransactionMapperTest() {
            winningTransactionMapper = new WinningTransactionDaoImpl.WinningTransactionMapper();
        }

        @Test
        public void mapRowOK() throws SQLException {
            WinningTransaction winningTransaction = WinningTransaction.builder()
                    .idTrxAcquirer("idTrxAcquirer")
                    .acquirerCode("acquirerCode")
                    .trxDate(OffsetDateTime.now())
                    .operationType("operationType")
                    .acquirerId("acquirerId")
                    .fiscalCode("fiscalCode")
                    .amount(BigDecimal.TEN)
                    .score(BigDecimal.ONE)
                    .build();

            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("id_trx_acquirer_s"))
                        .thenReturn(winningTransaction.getIdTrxAcquirer());
                Mockito.when(resultSet.getString("acquirer_c"))
                        .thenReturn(winningTransaction.getAcquirerCode());
                Mockito.when(resultSet.getString("trx_timestamp_t"))
                        .thenReturn(winningTransaction.getTrxDate().toString());
                Mockito.when(resultSet.getString("operation_type_c"))
                        .thenReturn(winningTransaction.getOperationType());
                Mockito.when(resultSet.getString("acquirer_id_s"))
                        .thenReturn(winningTransaction.getAcquirerId());
                Mockito.when(resultSet.getString("fiscal_code_s"))
                        .thenReturn(winningTransaction.getFiscalCode());
                Mockito.when(resultSet.getBigDecimal("amount_i"))
                        .thenReturn(winningTransaction.getAmount());
                Mockito.when(resultSet.getBigDecimal("score_n"))
                        .thenReturn(winningTransaction.getScore());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            WinningTransaction result = winningTransactionMapper.mapRow(resultSet, 0);
            Assert.assertNotNull(result);
            Assert.assertEquals(winningTransaction.getIdTrxAcquirer(), result.getIdTrxAcquirer());
            Assert.assertEquals(winningTransaction.getAcquirerCode(), result.getAcquirerCode());
            Assert.assertEquals(winningTransaction.getTrxDate(), result.getTrxDate());
            Assert.assertEquals(winningTransaction.getOperationType(), result.getOperationType());
            Assert.assertEquals(winningTransaction.getAcquirerId(), result.getAcquirerId());
            Assert.assertEquals(winningTransaction.getFiscalCode(), result.getFiscalCode());
            Assert.assertEquals(winningTransaction.getAmount(), result.getAmount());
            Assert.assertEquals(winningTransaction.getScore(), result.getScore());
        }

        @Test(expected = DateTimeParseException.class)
        public void mapRowKO_invalidDateTimeFormat() throws SQLException {
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("trx_timestamp_t"))
                        .thenReturn("05/02/2021");
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            winningTransactionMapper.mapRow(resultSet, 0);
        }

    }
}