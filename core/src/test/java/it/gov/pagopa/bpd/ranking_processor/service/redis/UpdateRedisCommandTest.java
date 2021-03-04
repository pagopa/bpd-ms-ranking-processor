package it.gov.pagopa.bpd.ranking_processor.service.redis;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class UpdateRedisCommandTest {

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private static boolean getCashbackWorkerCountResult;
    private static boolean getRankingWorkerCountResult;
    private static boolean processResult;

    private final UpdateRedisCommand updateRedisCommand;
    private final CitizenRankingDao citizenRankingDaoMock;

    public UpdateRedisCommandTest() {
        citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        when(citizenRankingDaoMock.registerWorker(eq(UPDATE_REDIS), eq(true)))
                .thenAnswer(invocationOnMock -> registerWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.unregisterWorker(eq(UPDATE_REDIS)))
                .thenAnswer(invocationOnMock -> unregisterWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_CASHBACK)))
                .thenAnswer(invocationOnMock -> getCashbackWorkerCountResult ? 0 : 1);
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_RANKING)))
                .thenAnswer(invocationOnMock -> getRankingWorkerCountResult ? 0 : 1);
        when(citizenRankingDaoMock.updateRedis())
                .thenAnswer(invocationOnMock -> processResult ? 1 : -1);

        updateRedisCommand = new UpdateRedisCommand(citizenRankingDaoMock);
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
        getCashbackWorkerCountResult = true;
        getRankingWorkerCountResult = true;
        processResult = true;
    }

    @Test
    public void execute_Ok() {
        updateRedisCommand.execute(null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_REDIS), eq(true));
        verify(citizenRankingDaoMock, times(1)).updateRedis();
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_REDIS));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test(expected = RedisUpdateException.class)
    public void execute_KoUpdateRedisFails() {
        processResult = false;

        try {
            updateRedisCommand.execute(null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to update Redis table", e.getMessage());
            throw e;
        }

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_REDIS), eq(true));
        verify(citizenRankingDaoMock, times(1)).updateRedis();
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_OkUpdateCashbackIncomplete() {
        getCashbackWorkerCountResult = false;

        updateRedisCommand.execute(null);

        verify(citizenRankingDaoMock, only()).getWorkerCount(eq(UPDATE_CASHBACK));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_OkUpdateRankingIncomplete() {
        getRankingWorkerCountResult = false;

        updateRedisCommand.execute(null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test(expected = RedisUpdateException.class)
    public void execute_KoRegisterWorker() {
        registerWorkerResult = false;
        try {
            updateRedisCommand.execute(null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to register worker to process " + UPDATE_REDIS, e.getMessage());
            throw e;
        }
    }

    @Test(expected = RedisUpdateException.class)
    public void execute_KoUnregisterWorker() {
        unregisterWorkerResult = false;
        try {
            updateRedisCommand.execute(null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to unregister worker to process " + UPDATE_REDIS, e.getMessage());
            throw e;
        }
    }
}