package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategyFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.dao.DeadlockLoserDataAccessException;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.will;
import static org.mockito.Mockito.*;

public class UpdateCashbackCommandTest {

    public static final int MAX_RETRY = 2;

    private static boolean registerWorkerResult;
    private static boolean unregisterWorkerResult;
    private static boolean deadLock;
    private static boolean retry;
    private UpdateCashbackCommand updateCashbackCommand;
    private final CitizenRankingDao citizenRankingDaoMock;
    private final CashbackUpdateStrategyFactory strategyFactory;
    private final CashbackUpdateStrategy updateStrategyMock;

    public UpdateCashbackCommandTest() {
        citizenRankingDaoMock = mock(CitizenRankingDao.class);
        when(citizenRankingDaoMock.registerWorker(any(), anyBoolean()))
                .thenAnswer(invocationOnMock -> registerWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.unregisterWorker(any()))
                .thenAnswer(invocationOnMock -> unregisterWorkerResult ? 1 : -1);
        when(citizenRankingDaoMock.getWorkerCount(any()))
                .thenReturn(0);

        updateStrategyMock = mock(CashbackUpdateStrategy.class);
        will(invocationOnMock -> {
            if (deadLock) {
                deadLock = retry;
                throw mock(DeadlockLoserDataAccessException.class);
            }
            return 0;
        }).given(updateStrategyMock).process(any(), any());
        doReturn(2)
                .when(updateStrategyMock).getDataExtractionLimit();


        strategyFactory = mock(CashbackUpdateStrategyFactory.class);
        when(strategyFactory.create(Mockito.any()))
                .thenReturn(updateStrategyMock);

        updateCashbackCommand = new UpdateCashbackCommand(strategyFactory, citizenRankingDaoMock, MAX_RETRY, false);
    }

    @Before
    public void setUp() {
        registerWorkerResult = true;
        unregisterWorkerResult = true;
        deadLock = false;
        retry = false;
    }


    @Test
    public void execute_Ok() {
        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(3)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_OkWithDeadLock() {
        deadLock = true;

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length + 1)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_KoMaxRetry() {
        deadLock = true;
        retry = true;

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length * (1 + MAX_RETRY))).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_OkSkipTotalTransferDueToAlreadyProcessing() {
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER)))
                .thenReturn(1);
        updateCashbackCommand = new UpdateCashbackCommand(strategyFactory, citizenRankingDaoMock, MAX_RETRY, true);

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length - 2)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_OkSkipTotalTransferDueToExclusiveLock() {
        when(citizenRankingDaoMock.registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(true)))
                .thenReturn(0);

        updateCashbackCommand = new UpdateCashbackCommand(strategyFactory, citizenRankingDaoMock, MAX_RETRY, true);

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(2)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length - 1)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_OkSkipPartialTransferDueToPaymentsNotCompleted() {
        when(citizenRankingDaoMock.getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT)))
                .thenReturn(1);

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(false));
        verify(citizenRankingDaoMock, never()).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, never()).unregisterWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, never()).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length - 1)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test
    public void execute_OkSkipPartialTransferDueToExclusiveLock() {
        when(citizenRankingDaoMock.registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true)))
                .thenReturn(0);

        updateCashbackCommand.execute(null, null);

        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PAYMENT), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER), eq(false));
        verify(citizenRankingDaoMock, times(1)).registerWorker(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER), eq(true));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).unregisterWorker(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PAYMENT));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_TOTAL_TRANSFER));
        verify(citizenRankingDaoMock, times(1)).getWorkerCount(eq(UPDATE_CASHBACK_PARTIAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PAYMENT));
        verify(strategyFactory, times(1)).create(eq(TransactionType.TOTAL_TRANSFER));
        verify(strategyFactory, times(1)).create(eq(TransactionType.PARTIAL_TRANSFER));
        verify(updateStrategyMock, times(TransactionType.values().length - 1)).process(any(), any());
        verify(updateStrategyMock, atLeastOnce()).getDataExtractionLimit();
        verifyNoMoreInteractions(citizenRankingDaoMock, strategyFactory, updateStrategyMock);
    }


    @Test(expected = CashbackUpdateException.class)
    public void execute_KoRegisterWorker() {
        registerWorkerResult = false;
        try {
            updateCashbackCommand.execute(null, null);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("Failed to register worker to process "));
            throw e;
        }
    }


    @Test(expected = CashbackUpdateException.class)
    public void execute_KoUnregisterWorker() {
        unregisterWorkerResult = false;
        try {
            updateCashbackCommand.execute(null, null);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("Failed to unregister worker to process "));
            throw e;
        }
    }
}