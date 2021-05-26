package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.common.BaseTest;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.ActiveUserWinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

public class WinningTransactionDaoImplTest extends BaseTest {

    private final WinningTransactionDaoImpl winningWinningTransactionDao;
    private final JdbcTemplate jdbcTemplateMock;


    public WinningTransactionDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        winningWinningTransactionDao = new WinningTransactionDaoImpl(jdbcTemplateMock, true, "elab_ranking_b", "1 month");
    }


    @Test
    public void findPaymentToProcessOK_withPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPaymentToProcess(1L,
                PageRequest.of(0, 1));

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void findPaymentToProcessOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPaymentToProcess(1L,
                null);

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void findTransferToProcessOK_withPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
        filterCriteria.setAwardPeriodId(1L);
        filterCriteria.setUpdateDate(OffsetDateTime.now());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findTransferToProcess(filterCriteria,
                PageRequest.of(0, 1));

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }


    @Test
    public void findPaymentTrxWithCorrelationIdOK_found() {
        Mockito.when(jdbcTemplateMock.queryForObject(anyString(), any(RowMapper.class), any()))
                .thenReturn(TestUtils.mockInstance(WinningTransaction.builder().build()));

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        WinningTransaction transaction = winningWinningTransactionDao.findPaymentTrxWithCorrelationId(filterCriteria);

        Assert.assertNotNull(transaction);
    }

    @Test
    public void findPaymentTrxWithCorrelationIdOK_notFound() {
        doThrow(EmptyResultDataAccessException.class)
                .when(jdbcTemplateMock).queryForObject(anyString(), any(RowMapper.class), any());

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        WinningTransaction transaction = winningWinningTransactionDao.findPaymentTrxWithCorrelationId(filterCriteria);

        Assert.assertNull(transaction);
    }

    @Test
    public void findPaymentTrxWithoutCorrelationIdOK_found() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Arrays.asList(TestUtils.mockInstance(WinningTransaction.builder().build())));

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        WinningTransaction transaction = winningWinningTransactionDao.findPaymentTrxWithoutCorrelationId(filterCriteria);

        Assert.assertNotNull(transaction);
    }

    @Test
    public void findPaymentTrxWithoutCorrelationIdOK_foundMoreThanOne() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Arrays.asList(TestUtils.mockInstance(WinningTransaction.builder().build(), 1), TestUtils.mockInstance(WinningTransaction.builder().build(), 1)));

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        WinningTransaction transaction = winningWinningTransactionDao.findPaymentTrxWithoutCorrelationId(filterCriteria);

        Assert.assertNotNull(transaction);
    }

    @Test
    public void findPaymentTrxWithoutCorrelationIdOK_notFound() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        WinningTransaction transaction = winningWinningTransactionDao.findPaymentTrxWithoutCorrelationId(filterCriteria);

        Assert.assertNull(transaction);
    }


    @Test
    public void findTransferToProcessOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
        filterCriteria.setAwardPeriodId(1L);
        filterCriteria.setUpdateDate(OffsetDateTime.now());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findTransferToProcess(filterCriteria,
                null);

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void findPartialTranferToProcessOK_withPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPartialTranferToProcess(1L,
                PageRequest.of(0, 1));

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void findPartialTranferToProcessOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPartialTranferToProcess(1L,
                null);

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }


    @Test
    public void updateProcessedTransactionOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = winningWinningTransactionDao.updateProcessedTransaction(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }

    @Test
    public void findActiveUsersSinceLastDetectorOK(){
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<ActiveUserWinningTransaction> resultList = winningWinningTransactionDao.findActiveUsersSinceLastDetector(1L);

        Assert.assertNotNull(resultList);
        Assert.assertEquals(0, resultList.size());
    }

    @Test
    public void findTopValidWinningTransactionsOK(){
        Mockito.when(jdbcTemplateMock.query(anyString(), any(ResultSetExtractor.class), any()))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findTopValidWinningTransactions(1L, 5, "fiscalCode", "merchant", LocalDate.now());

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void updateDetectorLastExecutionOK(){
        int result = winningWinningTransactionDao.updateDetectorLastExecution(OffsetDateTime.now());

        Assert.assertEquals(0, result);
    }

    @Test
    public void updateInvalidateTransactionsOK(){
        int result = winningWinningTransactionDao.updateInvalidateTransactions("fiscalCode", LocalDate.now(), "dummyMerchant", 1L);

        Assert.assertEquals(0, result);
    }

    @Test
    public void updateSetValidTransactionsOK(){
        int[] affectedRows = winningWinningTransactionDao.updateSetValidTransactions(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }

    @Test
    public void updateUserTransactionsElabOK(){
        int result = winningWinningTransactionDao.updateUserTransactionsElab("fiscalCode", 1L);

        Assert.assertEquals(0, result);
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
                Mockito.when(resultSet.getObject("trx_timestamp_t", OffsetDateTime.class))
                        .thenReturn(winningTransaction.getTrxDate());
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
    }

    public static class WinningTransactionValidMapperTest {

        private final WinningTransactionDaoImpl.WinningTransactionValidMapper winningTransactionValidMapper;

        public WinningTransactionValidMapperTest() {
            winningTransactionValidMapper = new WinningTransactionDaoImpl.WinningTransactionValidMapper();
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
                    .build();

            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("id_trx_acquirer_s"))
                        .thenReturn(winningTransaction.getIdTrxAcquirer());
                Mockito.when(resultSet.getString("acquirer_c"))
                        .thenReturn(winningTransaction.getAcquirerCode());
                Mockito.when(resultSet.getObject("trx_timestamp_t", OffsetDateTime.class))
                        .thenReturn(winningTransaction.getTrxDate());
                Mockito.when(resultSet.getString("operation_type_c"))
                        .thenReturn(winningTransaction.getOperationType());
                Mockito.when(resultSet.getString("acquirer_id_s"))
                        .thenReturn(winningTransaction.getAcquirerId());
                Mockito.when(resultSet.getString("fiscal_code_s"))
                        .thenReturn(winningTransaction.getFiscalCode());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            WinningTransaction result = winningTransactionValidMapper.mapRow(resultSet, 0);
            Assert.assertNotNull(result);
            Assert.assertEquals(winningTransaction.getIdTrxAcquirer(), result.getIdTrxAcquirer());
            Assert.assertEquals(winningTransaction.getAcquirerCode(), result.getAcquirerCode());
            Assert.assertEquals(winningTransaction.getTrxDate(), result.getTrxDate());
            Assert.assertEquals(winningTransaction.getOperationType(), result.getOperationType());
            Assert.assertEquals(winningTransaction.getAcquirerId(), result.getAcquirerId());
            Assert.assertEquals(winningTransaction.getFiscalCode(), result.getFiscalCode());
        }
    }

    public static class ActiveUsersMapperTest {

        private final WinningTransactionDaoImpl.ActiveUsersMapper activeUsersMapper;

        public ActiveUsersMapperTest() {
            activeUsersMapper = new WinningTransactionDaoImpl.ActiveUsersMapper();
        }

        @Test
        public void mapRowOK() throws SQLException {
            ActiveUserWinningTransaction activeUserWinningTransaction = ActiveUserWinningTransaction.builder()
                    .fiscalCode("dummyFiscalCode")
                    .merchantId("dummyMerchantId")
                    .trxDate(LocalDate.now())
                    .insertDate(OffsetDateTime.now())
                    .build();

            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("fiscal_code_s"))
                        .thenReturn(activeUserWinningTransaction.getFiscalCode());
                Mockito.when(resultSet.getString("merchant_id_s"))
                        .thenReturn(activeUserWinningTransaction.getMerchantId());
                Mockito.when(resultSet.getObject("payment_day", OffsetDateTime.class))
                        .thenReturn(OffsetDateTime.now());
                Mockito.when(resultSet.getObject("insert_date_t", OffsetDateTime.class))
                        .thenReturn(activeUserWinningTransaction.getInsertDate());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            ActiveUserWinningTransaction result = activeUsersMapper.mapRow(resultSet, 0);
            Assert.assertNotNull(result);
            Assert.assertEquals(activeUserWinningTransaction.getFiscalCode(), result.getFiscalCode());
            Assert.assertEquals(activeUserWinningTransaction.getMerchantId(), result.getMerchantId());
            Assert.assertEquals(activeUserWinningTransaction.getTrxDate(), result.getTrxDate());
            Assert.assertEquals(activeUserWinningTransaction.getInsertDate(), result.getInsertDate());
        }
    }
}