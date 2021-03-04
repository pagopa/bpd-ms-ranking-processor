package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_CASHBACK;
import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_RANKING;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class UpdateRankingCommandTest {

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private static boolean getWorkerCountResult;

    private final UpdateRankingCommand updateRankingCommand;
    private final CitizenRankingDao citizenRankingDaoMock;
    private final RankingUpdateStrategyFactory strategyFactoryMock;

    public UpdateRankingCommandTest() {
        citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        when(citizenRankingDaoMock.registerWorker(eq(UPDATE_RANKING), eq(true)))
                .thenAnswer(invocationOnMock -> registerWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.unregisterWorker(eq(UPDATE_RANKING)))
                .thenAnswer(invocationOnMock -> unregisterWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_CASHBACK)))
                .thenAnswer(invocationOnMock -> getWorkerCountResult ? 0 : 1);

        strategyFactoryMock = Mockito.mock(RankingUpdateStrategyFactory.class);
        when(strategyFactoryMock.create())
                .thenReturn(Mockito.mock(RankingUpdateStrategy.class));

        updateRankingCommand = new UpdateRankingCommand(strategyFactoryMock, citizenRankingDaoMock, 2);
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
        getWorkerCountResult = true;
    }

    @Test
    public void execute_Ok() {
        updateRankingCommand.execute(Mockito.mock(AwardPeriod.class));

        BDDMockito.verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK));
        BDDMockito.verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_RANKING), eq(true));
        BDDMockito.verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_RANKING));
        BDDMockito.verify(strategyFactoryMock, only()).create();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactoryMock);
    }

    @Test
    public void execute_OkUpdateCashbackIncomplete() {
        getWorkerCountResult = false;

        updateRankingCommand.execute(null);

        BDDMockito.verify(citizenRankingDaoMock, only()).getWorkerCount(eq(UPDATE_CASHBACK));
        verifyNoMoreInteractions(citizenRankingDaoMock);
        verifyZeroInteractions(strategyFactoryMock);
    }

    @Test(expected = RankingUpdateException.class)
    public void execute_KoRegisterWorker() {
        registerWorkerResult = false;
        try {
            updateRankingCommand.execute(Mockito.mock(AwardPeriod.class));
        } catch (Exception e) {
            Assert.assertEquals("Failed to register worker to process " + UPDATE_RANKING, e.getMessage());
            throw e;
        }
    }

    @Test(expected = RankingUpdateException.class)
    public void execute_KoUnregisterWorker() {
        unregisterWorkerResult = false;
        try {
            updateRankingCommand.execute(Mockito.mock(AwardPeriod.class));
        } catch (Exception e) {
            Assert.assertEquals("Failed to unregister worker to process " + UPDATE_RANKING, e.getMessage());
            throw e;
        }
    }
}