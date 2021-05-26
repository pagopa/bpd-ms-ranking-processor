package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;


import it.gov.pagopa.bpd.common.BaseTest;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRankingExt;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsertOperations;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class CitizenRankingDaoImplTest extends BaseTest {

    private final JdbcTemplate jdbcTemplateMock;
    private final CitizenRankingDaoImpl citizenRankingDao;
    private final SimpleJdbcInsertOperations insertRankingOpsMock;
    private final SimpleJdbcInsertOperations insertRankingExtOpsMock;

    @SneakyThrows
    public CitizenRankingDaoImplTest() {
        jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
        citizenRankingDao = new CitizenRankingDaoImpl(jdbcTemplateMock,
                "bpd_citizen_ranking",
                "bpd_citizen_ranking_ext",
                "bpd_ranking_processor_lock",
                "update_ranking_with_milestone");
        Field insertRankingOps = CitizenRankingDaoImpl.class.getDeclaredField("insertRankingOps");
        insertRankingOps.setAccessible(true);
        insertRankingOpsMock = Mockito.mock(SimpleJdbcInsertOperations.class);
        insertRankingOps.set(citizenRankingDao, insertRankingOpsMock);
        Field insertRankingExtOps = CitizenRankingDaoImpl.class.getDeclaredField("insertRankingExtOps");
        insertRankingExtOps.setAccessible(true);
        insertRankingExtOpsMock = Mockito.mock(SimpleJdbcInsertOperations.class);
        insertRankingExtOps.set(citizenRankingDao, insertRankingOpsMock);
    }


    @Test
    public void updateCashbackOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = citizenRankingDao.updateCashback(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }


    @Test
    public void insertCashbackOK() {
        Mockito.when(insertRankingOpsMock.executeBatch(any(SqlParameterSource[].class)))
                .thenReturn(new int[]{});

        int[] affectedRows = citizenRankingDao.insertCashback(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }


    @Test
    public void findAllOK_withPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        PageRequest pageRequest = PageRequest.of(0, 1, Sort.Direction.ASC, "pippo");
        CitizenRanking.FilterCriteria filterCriteria = new CitizenRanking.FilterCriteria(1L, OffsetDateTime.now());
        List<CitizenRanking> results = citizenRankingDao.findAll(filterCriteria, pageRequest);

        Assert.assertNotNull(results);
    }

    @Test
    public void findAllOK_withoutPage() {
        Mockito.when(jdbcTemplateMock.query(any(PreparedStatementCreator.class), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenReturn(Collections.emptyList());

        CitizenRanking.FilterCriteria filterCriteria = new CitizenRanking.FilterCriteria(1L, OffsetDateTime.now());
        List<CitizenRanking> results = citizenRankingDao.findAll(filterCriteria, (Pageable) null);

        Assert.assertNotNull(results);
    }


    @Test
    public void updateRankingOK() {
        Mockito.when(jdbcTemplateMock.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[]{});

        int[] affectedRows = citizenRankingDao.updateRanking(Collections.emptyList());

        Assert.assertNotNull(affectedRows);
        Assert.assertEquals(0, affectedRows.length);
    }


    @Test
    public void updateRankingExtOK() {
        int result = citizenRankingDao.updateRankingExt(CitizenRankingExt.builder().build());

        Assert.assertEquals(0, result);
    }


    @Test
    public void insertRankingExtOK() {
        int result = citizenRankingDao.insertRankingExt(CitizenRankingExt.builder().build());

        Assert.assertEquals(0, result);
    }


    @Test
    public void resetCashbackOK() {
        int result = citizenRankingDao.resetCashback("fiscalCode", -1L);

        Assert.assertEquals(0, result);
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
        }

    }

}