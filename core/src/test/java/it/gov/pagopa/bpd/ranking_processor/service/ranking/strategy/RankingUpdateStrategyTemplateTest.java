package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.ranking.RankingUpdateException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public abstract class RankingUpdateStrategyTemplateTest {

    private static final int LIMIT = 5;

    protected final CitizenRankingDao citizenRankingDaoMock;

    private final ArgumentCaptor<LoggingEvent> loggingEventCaptor;
    private final ArgumentCaptor<CitizenRanking.FilterCriteria> filterCriteriaCaptor;
    private final ArgumentCaptor<List<CitizenRanking>> citizenRankingCaptor;
    private final Appender mockedAppender;

    private Error error;
    private MissRecord missRecords;


    public RankingUpdateStrategyTemplateTest() {
        this.citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        this.loggingEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        this.filterCriteriaCaptor = ArgumentCaptor.forClass(CitizenRanking.FilterCriteria.class);
        this.citizenRankingCaptor = ArgumentCaptor.forClass(List.class);
        this.mockedAppender = Mockito.mock(Appender.class);

        initMocks();
    }


    private void initMocks() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(mockedAppender);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
        ((Logger) LoggerFactory.getLogger("it.gov.pagopa.bpd")).setLevel(Level.DEBUG);

        OffsetDateTime start =OffsetDateTime.now();
        when(citizenRankingDaoMock.findAll(any(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<CitizenRanking> rankings = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        rankings.add(TestUtils.mockInstance(CitizenRanking.builder()
                                .transactionNumber(1L).lastTrxTimestamp(i<2?start.minusDays(1+i):null).timestampTc(i<4?start.minusDays(i):null).insertUser(""+i)
                                .build(), i, "setTransactionNumber", "setLastTrxTimestamp", "setTimestampTc", "setInsertUser"));
                    }
                    return rankings;
                });
        when(citizenRankingDaoMock.getUserTcTimestamp(Mockito.anyString())).thenReturn(OffsetDateTime.now().minusDays(5));


        when(citizenRankingDaoMock.updateRanking(anyCollection()))
                .thenAnswer(invocationOnMock -> {
                    Collection<CitizenRanking> rankings = invocationOnMock.getArgument(0, Collection.class);
                    int size = rankings.size();
                    if (!rankings.isEmpty() && Error.UPDATE_RANKING == error) {
                        size--;
                    }
                    int[] result = new int[size];
                    Arrays.fill(result, 1);
                    if (!rankings.isEmpty() && MissRecord.UPDATE_RANKING == missRecords) {
                        result[0] = 0;
                    }
                    return result;
                });
    }


    @Before
    public void init() {
        error = null;
        missRecords = null;
    }


    @Test
    public void process_OK() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder().awardPeriodId(1L).minPosition(2L).build();
        int processedTrxCount = getRankingUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verify(citizenRankingDaoMock, times(1))
                .findAll(filterCriteriaCaptor.capture(), eq(toPageable(pageRequest)));
        OffsetDateTime updateDate = null;
        for (CitizenRanking.FilterCriteria filterCriteria : filterCriteriaCaptor.getAllValues()) {
            Assert.assertEquals(awardPeriod.getAwardPeriodId(), filterCriteria.getAwardPeriodId());
            if (updateDate == null) {
                updateDate = filterCriteria.getUpdateDate();
            } else {
                Assert.assertEquals(updateDate, filterCriteria.getUpdateDate());
            }
        }
        verify(citizenRankingDaoMock, times(1))
                .updateRanking(citizenRankingCaptor.capture());

        for (CitizenRanking citizen : citizenRankingCaptor.getValue()) {
            Assert.assertEquals(5L-Long.parseLong(citizen.getInsertUser()),citizen.getRanking().longValue());
        }

        verify(citizenRankingDaoMock, times(1))
                .getUserTcTimestamp(anyString());
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void process_OK_noTimestampTc() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder().awardPeriodId(1L).minPosition(2L).build();
        when(citizenRankingDaoMock.getUserTcTimestamp(Mockito.eq("setFiscalCode4"))).thenReturn(null);

        int processedTrxCount = getRankingUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verify(citizenRankingDaoMock, times(1))
                .findAll(filterCriteriaCaptor.capture(), eq(toPageable(pageRequest)));
        OffsetDateTime updateDate = null;
        for (CitizenRanking.FilterCriteria filterCriteria : filterCriteriaCaptor.getAllValues()) {
            Assert.assertEquals(awardPeriod.getAwardPeriodId(), filterCriteria.getAwardPeriodId());
            if (updateDate == null) {
                updateDate = filterCriteria.getUpdateDate();
            } else {
                Assert.assertEquals(updateDate, filterCriteria.getUpdateDate());
            }
        }
        verify(citizenRankingDaoMock, times(1))
                .updateRanking(citizenRankingCaptor.capture());

        for (CitizenRanking citizen : citizenRankingCaptor.getValue()) {
            switch(citizen.getInsertUser()){
                case "0":
                    Assert.assertEquals(5L,citizen.getRanking().longValue());
                    break;
                case "1":
                    Assert.assertEquals(4L,citizen.getRanking().longValue());
                    break;
                case "2":
                    Assert.assertEquals(2L,citizen.getRanking().longValue());
                    break;
                case "3":
                    Assert.assertEquals(1L,citizen.getRanking().longValue());
                    break;
                case "4":
                    Assert.assertEquals(3L,citizen.getRanking().longValue());
                    break;
            }
        }

        verify(citizenRankingDaoMock, times(1))
                .getUserTcTimestamp(anyString());
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    private static PageRequest toPageable(SimplePageRequest pageRequest) {
        return PageRequest.of(pageRequest.getPage(), pageRequest.getSize(), CitizenRankingDao.FIND_ALL_PAGEABLE_SORT);
    }

    @Test(expected = RankingUpdateException.class)
    public void process_KO_cashbackUpdateMiss() {
        missRecords = MissRecord.UPDATE_RANKING;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder().awardPeriodId(1L).minPosition(2L).build();

        try {
            getRankingUpdateService().process(awardPeriod, pageRequest);

        } catch (RankingUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            verify(citizenRankingDaoMock, times(1))
                    .findAll(filterCriteriaCaptor.capture(), eq(toPageable(pageRequest)));
            OffsetDateTime updateDate = null;
            for (CitizenRanking.FilterCriteria filterCriteria : filterCriteriaCaptor.getAllValues()) {
                Assert.assertEquals(awardPeriod.getAwardPeriodId(), filterCriteria.getAwardPeriodId());
                if (updateDate == null) {
                    updateDate = filterCriteria.getUpdateDate();
                } else {
                    Assert.assertEquals(updateDate, filterCriteria.getUpdateDate());
                }
            }
            verify(citizenRankingDaoMock, times(1))
                    .updateRanking(anyList());
            verify(citizenRankingDaoMock, times(1))
                    .getUserTcTimestamp(anyString());
            verifyNoMoreInteractions(citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = RankingUpdateException.class)
    public void process_KO_cashbackUpdateError() {
        error = Error.UPDATE_RANKING;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder().awardPeriodId(1L).minPosition(2L).build();

        try {
            getRankingUpdateService().process(awardPeriod, pageRequest);

        } catch (RankingUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            verify(citizenRankingDaoMock, times(1))
                    .findAll(filterCriteriaCaptor.capture(), eq(toPageable(pageRequest)));
            OffsetDateTime updateDate = null;
            for (CitizenRanking.FilterCriteria filterCriteria : filterCriteriaCaptor.getAllValues()) {
                Assert.assertEquals(awardPeriod.getAwardPeriodId(), filterCriteria.getAwardPeriodId());
                if (updateDate == null) {
                    updateDate = filterCriteria.getUpdateDate();
                } else {
                    Assert.assertEquals(updateDate, filterCriteria.getUpdateDate());
                }
            }
            verify(citizenRankingDaoMock, times(1))
                    .updateRanking(anyList());
            verify(citizenRankingDaoMock, times(1))
                    .getUserTcTimestamp(anyString());
            verifyNoMoreInteractions(citizenRankingDaoMock);

            throw e;
        }
    }

    public abstract RankingUpdateStrategy getRankingUpdateService();

    private enum Error {
        UPDATE_RANKING
    }


    private enum MissRecord {
        UPDATE_RANKING
    }

}