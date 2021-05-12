package it.gov.pagopa.bpd.ranking_processor.service.milestone;


import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class UpdateMilestoneCommandTest {

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private static boolean getCashbackWorkerCountResult;
    private static boolean getRankingWorkerCountResult;
    private static boolean processResult;
    private static boolean singleprocess;

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
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
        getCashbackWorkerCountResult = true;
        getRankingWorkerCountResult = true;
        processResult = true;
        updateMilestoneCommand = new UpdateMilestoneCommand(citizenRankingDaoMock, 2, 100, 10, true, 100);
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
    public void execute_OkNoSingleProcess() {
        getRankingWorkerCountResult = false;
        updateMilestoneCommand = new UpdateMilestoneCommand(citizenRankingDaoMock, 2, 100, 10, false, 100);
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