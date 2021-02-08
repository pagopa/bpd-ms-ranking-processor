package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import it.gov.pagopa.bpd.common.BaseTest;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
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


    public static class CitizenRankingMapperTest {

        private final CitizenRankingDaoImpl.CitizenRankingMapper citizenRankingMapper;

        public CitizenRankingMapperTest() {
            citizenRankingMapper = new CitizenRankingDaoImpl.CitizenRankingMapper();
        }

        @Test
        public void mapRowOK() throws SQLException {
            CitizenRanking citizenRanking = CitizenRanking.builder()
                    .fiscalCode("fiscalCode")
                    .awardPeriodId(1L)
                    .totalCashback(BigDecimal.ONE)
                    .transactionNumber(1L)
                    .ranking(1L)
                    .rankingMinRequired(1L)
                    .maxTotalCashback(BigDecimal.ONE)
                    .build();

            ResultSet resultSet = Mockito.mock(ResultSet.class);
            try {
                Mockito.when(resultSet.getString("fiscal_code_c"))
                        .thenReturn(citizenRanking.getFiscalCode());
                Mockito.when(resultSet.getLong("award_period_id_n"))
                        .thenReturn(citizenRanking.getAwardPeriodId());
                Mockito.when(resultSet.getBigDecimal("cashback_n"))
                        .thenReturn(citizenRanking.getTotalCashback());
                Mockito.when(resultSet.getLong("transaction_n"))
                        .thenReturn(citizenRanking.getTransactionNumber());
                Mockito.when(resultSet.getLong("ranking_n"))
                        .thenReturn(citizenRanking.getRanking());
                Mockito.when(resultSet.getLong("ranking_min_n"))
                        .thenReturn(citizenRanking.getRankingMinRequired());
                Mockito.when(resultSet.getBigDecimal("max_cashback_n"))
                        .thenReturn(citizenRanking.getMaxTotalCashback());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                Assert.fail(throwables.getMessage());
            }

            CitizenRanking result = citizenRankingMapper.mapRow(resultSet, 0);

            Assert.assertNotNull(result);
            Assert.assertEquals(citizenRanking.getFiscalCode(), result.getFiscalCode());
            Assert.assertEquals(citizenRanking.getAwardPeriodId(), result.getAwardPeriodId());
            Assert.assertEquals(citizenRanking.getTotalCashback(), result.getTotalCashback());
            Assert.assertEquals(citizenRanking.getTransactionNumber(), result.getTransactionNumber());
            Assert.assertEquals(citizenRanking.getRanking(), result.getRanking());
            Assert.assertEquals(citizenRanking.getRankingMinRequired(), result.getRankingMinRequired());
            Assert.assertEquals(citizenRanking.getMaxTotalCashback(), result.getMaxTotalCashback());
        }

    }

}