package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import it.gov.pagopa.bpd.common.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class CitizenRankingDaoImplTest extends BaseTest {

    private final JdbcTemplate jdbcTemplateMock;
    private final CitizenRankingDaoImpl citizenRankingDao;

    public CitizenRankingDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        citizenRankingDao = new CitizenRankingDaoImpl(jdbcTemplateMock);
    }


    @Test
    public void updateProcessedTransactionOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = citizenRankingDao.updateCashback(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }

}