package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.*;

import static it.gov.pagopa.bpd.ranking_processor.service.ranking.RankingUpdateStrategyTemplate.ERROR_MESSAGE_TEMPLATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public abstract class RankingUpdateStrategyTemplateTest {

    private static final int LIMIT = 5;

    protected final CitizenRankingDao citizenRankingDaoMock;

    private final ArgumentCaptor<LoggingEvent> loggingEventCaptor;
    private final Appender mockedAppender;

    private Error error;
    private MissRecord missRecords;

    private void initMocks() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(mockedAppender);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
        ((Logger) LoggerFactory.getLogger("it.gov.pagopa.bpd")).setLevel(Level.DEBUG);

        Random random = new Random();
        when(citizenRankingDaoMock.findAll(anyLong(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<CitizenRanking> rankings = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        rankings.add(TestUtils.mockInstance(CitizenRanking.builder()
                                .transactionNumber(random.nextLong())
                                .build(), i, "setTransactionNumber"));
                    }
                    return rankings;
                });

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


    public RankingUpdateStrategyTemplateTest() {
        this.citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        this.loggingEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        this.mockedAppender = Mockito.mock(Appender.class);

        initMocks();
    }

    @Test
    public void process_OK() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;
        int processedTrxCount = getRankingUpdateService().process(awardPeriodId, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .findAll(eq(awardPeriodId), eq(toPageable(pageRequest)));
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateRanking(anyList());
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    private static PageRequest toPageable(SimplePageRequest pageRequest) {
        return PageRequest.of(pageRequest.getPage(), pageRequest.getSize(), CitizenRankingDao.SORT_BY_TRX_NUM_DESC);
    }

    @Test(expected = RankingUpdateException.class)
    public void process_KO_cashbackUpdateMiss() {
        missRecords = MissRecord.UPDATE_RANKING;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getRankingUpdateService().process(awardPeriodId, pageRequest);

        } catch (RankingUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            LIMIT - 1,
                            LIMIT).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .findAll(eq(awardPeriodId), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateRanking(anyList());
            verifyNoMoreInteractions(citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = RankingUpdateException.class)
    public void process_KO_cashbackUpdateError() {
        error = Error.UPDATE_RANKING;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getRankingUpdateService().process(awardPeriodId, pageRequest);

        } catch (RankingUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            LIMIT - 1,
                            LIMIT).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .findAll(eq(awardPeriodId), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateRanking(anyList());
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