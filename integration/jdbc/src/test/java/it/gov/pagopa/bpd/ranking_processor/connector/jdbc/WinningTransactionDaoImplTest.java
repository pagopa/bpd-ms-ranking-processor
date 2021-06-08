package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.common.BaseTest;
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
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;

public class WinningTransactionDaoImplTest extends BaseTest {

    private final WinningTransactionDaoImpl winningWinningTransactionDao;
    private final JdbcTemplate jdbcTemplateMock;


    public WinningTransactionDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        winningWinningTransactionDao = new WinningTransactionDaoImpl(jdbcTemplateMock, true, "elab_ranking_b", "bpd_winning_transaction_transfer");
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
    public void findProcessedTranferAmountOK_found() {
        Mockito.when(jdbcTemplateMock.queryForObject(anyString(), eq(BigDecimal.class), any()))
                .thenReturn(BigDecimal.TEN);

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        BigDecimal processedTranferAmount = winningWinningTransactionDao.findProcessedTransferAmount(filterCriteria);

        Assert.assertNotNull(processedTranferAmount);
        Assert.assertEquals(BigDecimal.TEN, processedTranferAmount);
    }


    @Test
    public void findProcessedTranferAmountOK_notFound() {
        doThrow(EmptyResultDataAccessException.class)
                .when(jdbcTemplateMock).queryForObject(anyString(), eq(BigDecimal.class), any());

        WinningTransaction.FilterCriteria filterCriteria = TestUtils.mockInstance(new WinningTransaction.FilterCriteria());
        BigDecimal processedTranferAmount = winningWinningTransactionDao.findProcessedTransferAmount(filterCriteria);

        Assert.assertNull(processedTranferAmount);
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

        WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
        filterCriteria.setAwardPeriodId(1L);
        filterCriteria.setUpdateDate(OffsetDateTime.now());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPartialTransferToProcess(filterCriteria,
                PageRequest.of(0, 1));

        Assert.assertNotNull(transactions);
        Assert.assertEquals(0, transactions.size());
    }

    @Test
    public void findPartialTranferToProcessOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
        filterCriteria.setAwardPeriodId(1L);
        filterCriteria.setUpdateDate(OffsetDateTime.now());

        List<WinningTransaction> transactions = winningWinningTransactionDao.findPartialTransferToProcess(filterCriteria,
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
    public void deleteTransferOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = winningWinningTransactionDao.deleteTransfer(Collections.emptyList());

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
}