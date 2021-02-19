package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
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

import static it.gov.pagopa.bpd.ranking_processor.service.cashback.CashbackUpdateStrategyTemplate.ERROR_MESSAGE_TEMPLATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public abstract class CashbackUpdateStrategyTemplateTest {

    private static final int LIMIT = 5;

    protected final WinningTransactionDao winningTransactionDaoMock;
    protected final CitizenRankingDao citizenRankingDaoMock;

    private final Appender mockedAppender;
    private final ArgumentCaptor<LoggingEvent> loggingEventCaptor;

    private Error error;
    private EnumSet<MissRecord> missRecords = EnumSet.noneOf(MissRecord.class);

    private void initMocks() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(mockedAppender);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
        ((Logger) LoggerFactory.getLogger("it.gov.pagopa.bpd")).setLevel(Level.DEBUG);

        when(winningTransactionDaoMock.findTransactionToProcess(anyLong(), any(TransactionType.class), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    TransactionType trxType = invocationOnMock.getArgument(1, TransactionType.class);
                    Pageable pageable = invocationOnMock.getArgument(2, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        transactions.add(TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType(TransactionType.PAYMENT.equals(trxType) ? "00" : "01")
                                .build(), i, "setOperationType"));
                    }
                    return transactions;
                });

        when(citizenRankingDaoMock.updateCashback(anyList()))
                .thenAnswer(invocationOnMock -> {
                    Collection<CitizenRanking> rankings = invocationOnMock.getArgument(0, Collection.class);
                    int size = rankings.size();
                    if (!rankings.isEmpty() && Error.UPDATE_CASHBACK == error) {
                        size--;
                    }
                    int[] result = new int[size];
                    Arrays.fill(result, 1);
                    if (!rankings.isEmpty() && missRecords.contains(MissRecord.UPDATE_CASHBACK)) {
                        result[0] = 0;
                    }
                    return result;
                });

        when(citizenRankingDaoMock.insertCashback(anyList()))
                .thenAnswer(invocationOnMock -> {
                    Collection<CitizenRanking> rankings = invocationOnMock.getArgument(0, Collection.class);
                    int size = rankings.size();
                    if (!rankings.isEmpty() && Error.INSERT_CASHBACK == error) {
                        size--;
                    }
                    int[] result = new int[size];
                    Arrays.fill(result, 1);
                    if (!rankings.isEmpty() && missRecords.contains(MissRecord.INSERT_CASHBACK)) {
                        result[0] = 0;
                    }
                    return result;
                });

        when(winningTransactionDaoMock.updateProcessedTransaction(anyCollection()))
                .thenAnswer(invocationOnMock -> {
                    Collection argument = invocationOnMock.getArgument(0, Collection.class);
                    int size = argument.size();
                    if (!argument.isEmpty() && Error.UPDATE_TRANSACTION == error) {
                        size--;
                    }
                    int[] result = new int[size];
                    Arrays.fill(result, 1);
                    if (!argument.isEmpty() && missRecords.contains(MissRecord.UPDATE_TRANSACTION)) {
                        result[0] = 0;
                    }
                    return result;
                });
    }

    private static PageRequest toPageable(SimplePageRequest pageRequest) {
        return PageRequest.of(pageRequest.getPage(), pageRequest.getSize());
    }

    public CashbackUpdateStrategyTemplateTest() {
        this.winningTransactionDaoMock = Mockito.mock(WinningTransactionDao.class);
        this.citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        this.loggingEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        this.mockedAppender = Mockito.mock(Appender.class);

        initMocks();
    }

    @Test
    public void process_OK() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;
        int processedTrxCount = getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateCashback(anyList());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .insertCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    public abstract CashbackUpdateStrategy getCashbackUpdateService();

    @Before
    public void init() {
        error = null;
        missRecords.clear();
    }

    @Test
    public void process_OK_updateCashbackMiss() {
        missRecords.add(MissRecord.UPDATE_CASHBACK);
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;
        int processedTrxCount = getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateCashback(anyList());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .insertCashback(anyList());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateProcessedTransaction(anyCollection());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test(expected = CashbackUpdateException.class)
    public void process_KO_updateCashbackError() {
        error = Error.UPDATE_CASHBACK;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        } catch (CashbackUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            "updateCashback",
                            LIMIT - 1,
                            LIMIT).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
            verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = CashbackUpdateException.class)
    public void process_KO_insertCashbackError() {
        missRecords.add(MissRecord.UPDATE_CASHBACK);
        error = Error.INSERT_CASHBACK;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        } catch (CashbackUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            "insertCashback",
                            0,
                            1).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .insertCashback(anyList());
            verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = CashbackUpdateException.class)
    public void process_KO_insertCashbackMiss() {
        missRecords.add(MissRecord.UPDATE_CASHBACK);
        missRecords.add(MissRecord.INSERT_CASHBACK);
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        } catch (CashbackUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            "insertCashback",
                            0,
                            1).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .insertCashback(anyList());
            verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = CashbackUpdateException.class)
    public void process_KO_updateTransactionError() {
        error = Error.UPDATE_TRANSACTION;
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        } catch (CashbackUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            "updateProcessedTransaction",
                            LIMIT - 1,
                            LIMIT).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .insertCashback(anyList());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .updateProcessedTransaction(anyCollection());
            verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);

            throw e;
        }
    }

    @Test(expected = CashbackUpdateException.class)
    public void process_KO_updateTransactionMissing() {
        missRecords.add(MissRecord.UPDATE_TRANSACTION);
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        long awardPeriodId = 1L;

        try {
            getCashbackUpdateService().process(awardPeriodId, TransactionType.PAYMENT, pageRequest);

        } catch (CashbackUpdateException e) {
            verify(mockedAppender, atLeast(1)).doAppend(loggingEventCaptor.capture());
            Optional<LoggingEvent> errorEvent = loggingEventCaptor.getAllValues()
                    .stream()
                    .filter(loggingEvent -> Level.ERROR.equals(loggingEvent.getLevel()))
                    .filter(loggingEvent -> String.format(ERROR_MESSAGE_TEMPLATE,
                            "updateProcessedTransaction",
                            LIMIT - 1,
                            LIMIT).equals(loggingEvent.getFormattedMessage()))
                    .findAny();
            Assert.assertTrue(errorEvent.isPresent());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .findTransactionToProcess(eq(awardPeriodId), eq(TransactionType.PAYMENT), eq(toPageable(pageRequest)));
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .insertCashback(anyList());
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .updateProcessedTransaction(anyCollection());
            verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);

            throw e;
        }
    }


    private enum Error {
        UPDATE_CASHBACK,
        INSERT_CASHBACK,
        UPDATE_TRANSACTION
    }


    private enum MissRecord {
        UPDATE_CASHBACK,
        INSERT_CASHBACK,
        UPDATE_TRANSACTION
    }

}