package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import it.gov.pagopa.bpd.common.BaseTest;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class WinningWinningTransactionDaoImplTest extends BaseTest {

    private final WinningWinningTransactionDaoImpl winningWinningTransactionDao;
    private final JdbcTemplate jdbcTemplateMock;

    public WinningWinningTransactionDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        winningWinningTransactionDao = new WinningWinningTransactionDaoImpl(jdbcTemplateMock, true);
    }

    @Test
    public void findTransactionToProcessOK_Payment() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        List<WinningTransaction> transactionToProcess =
                winningWinningTransactionDao.findTransactionToProcess(1L, WinningTransaction.TransactionType.PAYMENT);

        Assert.assertNotNull(transactionToProcess);
        Assert.assertEquals(0, transactionToProcess.size());
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

        private final WinningWinningTransactionDaoImpl.WinningTransactionMapper winningTransactionMapper;

        public WinningTransactionMapperTest() {
            winningTransactionMapper = new WinningWinningTransactionDaoImpl.WinningTransactionMapper();
        }

        @Test
        public void mapRowOK() throws SQLException {
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("trx_timestamp_t"))
                        .thenReturn(OffsetDateTime.now().toString());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            winningTransactionMapper.mapRow(resultSet, 0);
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