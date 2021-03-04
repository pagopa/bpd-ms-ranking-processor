package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategyFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.UPDATE_CASHBACK;

public class UpdateCashbackCommandTest {

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private final UpdateCashbackCommand updateCashbackCommand;

    public UpdateCashbackCommandTest() {
        CitizenRankingDao citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        BDDMockito.when(citizenRankingDaoMock.registerWorker(Mockito.eq(UPDATE_CASHBACK)))
                .thenAnswer(invocationOnMock -> registerWorkerResult ? 1 : -1);
        BDDMockito.when(citizenRankingDaoMock.unregisterWorker(Mockito.eq(UPDATE_CASHBACK)))
                .thenAnswer(invocationOnMock -> unregisterWorkerResult ? 1 : -1);

        CashbackUpdateStrategyFactory strategyFactory = Mockito.mock(CashbackUpdateStrategyFactory.class);
        BDDMockito.when(strategyFactory.create(Mockito.any()))
                .thenReturn(Mockito.mock(CashbackUpdateStrategy.class));

        updateCashbackCommand = new UpdateCashbackCommand(strategyFactory, citizenRankingDaoMock, 2);
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
    }

    @Test
    public void execute_Ok() {
        updateCashbackCommand.execute(null);
    }

    @Test(expected = CashbackUpdateException.class)
    public void execute_KoRegisterWorker() {
        registerWorkerResult = false;
        try {
            updateCashbackCommand.execute(null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to register worker to process UPDATE_CASHBACK", e.getMessage());
            throw e;
        }
    }

    @Test(expected = CashbackUpdateException.class)
    public void execute_KoUnregisterWorker() {
        unregisterWorkerResult = false;
        try {
            updateCashbackCommand.execute(null);
        } catch (Exception e) {
            Assert.assertEquals("Failed to unregister worker to process UPDATE_CASHBACK", e.getMessage());
            throw e;
        }
    }
}