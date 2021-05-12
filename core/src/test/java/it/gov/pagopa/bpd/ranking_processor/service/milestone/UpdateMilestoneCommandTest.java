package it.gov.pagopa.bpd.ranking_processor.service.milestone;


import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.dao.DeadlockLoserDataAccessException;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.will;
import static org.mockito.Mockito.*;

public class UpdateMilestoneCommandTest {

    public static final int MAX_RETRY = 2;

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private static boolean getCashbackWorkerCountResult;
    private static boolean getRankingWorkerCountResult;
    private static boolean processResult;
    private static boolean singleprocess;
    private static boolean deadlock;
    private static boolean retry;

    private UpdateMilestoneCommand updateMilestoneCommand;
    private final CitizenRankingDao citizenRankingDaoMock;

    public UpdateMilestoneCommandTest() {
        citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        when(citizenRankingDaoMock.registerWorker(eq(UPDATE_MILESTONE), anyBoolean()))
                .thenAnswer(invocationOnMock -> registerWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.unregisterWorker(eq(UPDATE_MILESTONE)))
                .thenAnswer(invocationOnMock -> unregisterWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_CASHBACK)))
                .thenAnswer(invocationOnMock -> getCashbackWorkerCountResult ? 0 : 1);
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_RANKING)))
                .thenAnswer(invocationOnMock -> {
                    boolean result = getRankingWorkerCountResult;
                    if (!singleprocess) {
                        getRankingWorkerCountResult = true;
                    }
                    return result ? 0 : 1;
                });
        when(citizenRankingDaoMock.updateMilestone(anyInt(), anyInt(), any()))
                .thenAnswer(invocationOnMock -> processResult ? 1 : -1);
        will(invocationOnMock -> {
            if (deadlock) {
                deadlock = retry;
                throw mock(DeadlockLoserDataAccessException.class);
            }
            return 0;
        }).given(citizenRankingDaoMock).updateMilestone(anyInt(), anyInt(), any());
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
        getCashbackWorkerCountResult = true;
        getRankingWorkerCountResult = true;
        processResult = true;
        deadlock = false;
        retry = false;
        singleprocess = true;
        updateMilestoneCommand = new UpdateMilestoneCommand(citizenRankingDaoMock, 2, 100, 10, true, 100, null);
    }

    @Test
    public void execute_Ok() {
        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_MILESTONE), eq(true));
        verify(citizenRankingDaoMock, atLeast(1)).updateMilestone(anyInt(), anyInt(), any());
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_MILESTONE));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_OkWithDeadLock() {
        deadlock = true;

        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_MILESTONE), eq(true));
        verify(citizenRankingDaoMock, atLeast(2)).updateMilestone(anyInt(), anyInt(), any());
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_MILESTONE));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_KoMaxRetry() {
        deadlock = true;
        deadlock = retry;

        updateMilestoneCommand = new UpdateMilestoneCommand(citizenRankingDaoMock, 2, 100, 10, true, 100, MAX_RETRY);
        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_MILESTONE), eq(true));
        verify(citizenRankingDaoMock, atLeast(MAX_RETRY)).updateMilestone(anyInt(), anyInt(), any());
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_MILESTONE));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_OkNoSingleProcess() {
        singleprocess = false;
        getRankingWorkerCountResult = false;
        updateMilestoneCommand = new UpdateMilestoneCommand(citizenRankingDaoMock, 2, 100, 10, false, 100, null);
        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_RANKING));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_MILESTONE), eq(false));
        verify(citizenRankingDaoMock, atLeast(1)).updateMilestone(anyInt(), anyInt(), any());
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_MILESTONE));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }


    @Test
    public void execute_OkUpdateCashbackIncomplete() {
        getCashbackWorkerCountResult = false;

        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, only()).getWorkerCount(eq(UPDATE_CASHBACK));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test
    public void execute_OkUpdateRankingIncomplete() {
        getRankingWorkerCountResult = false;

        updateMilestoneCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_RANKING));
        verifyNoMoreInteractions(citizenRankingDaoMock);
    }

    @Test(expected = MilestoneUpdateException.class)
    public void execute_KoRegisterWorker() {
        registerWorkerResult = false;
        try {
            updateMilestoneCommand.execute(null, null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to register worker to process " + UPDATE_MILESTONE, e.getMessage());
            throw e;
        }
    }

    @Test(expected = MilestoneUpdateException.class)
    public void execute_KoUnregisterWorker() {
        unregisterWorkerResult = false;
        try {
            updateMilestoneCommand.execute(null, null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to unregister worker to process " + UPDATE_MILESTONE, e.getMessage());
            throw e;
        }
    }

}