package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.CashbackUpdateException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.*;

import static it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategyTemplate.ERROR_MESSAGE_TEMPLATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public abstract class CashbackUpdateStrategyTemplateTest {

    private static final int LIMIT = 5;

    protected final WinningTransactionDao winningTransactionDaoMock;
    protected final CitizenRankingDao citizenRankingDaoMock;
    protected final AggregatorStrategy aggregatorStrategy;
    protected final BeanFactory beanFactoryMock;

    private final Appender mockedAppender;
    private final ArgumentCaptor<LoggingEvent> loggingEventCaptor;

    private Error error;
    private EnumSet<MissRecord> missRecords = EnumSet.noneOf(MissRecord.class);

    public CashbackUpdateStrategyTemplateTest() {
        this.winningTransactionDaoMock = Mockito.mock(WinningTransactionDao.class);
        this.citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        this.aggregatorStrategy = Mockito.mock(AggregatorStrategy.class);
        this.loggingEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        this.mockedAppender = Mockito.mock(Appender.class);
        beanFactoryMock = Mockito.mock(BeanFactory.class);
        BDDMockito.doAnswer(invocationOnMock -> {
            Class argument = invocationOnMock.getArgument(0, Class.class);
            if (CommonAggregator.class.getName().equals(argument.getName()))
                return aggregatorStrategy;
            else if (PartialTransferAggregator.class.getName().equals(argument.getName()))
                return aggregatorStrategy;
            else
                throw new IllegalArgumentException();
        })
                .when(beanFactoryMock)
                .getBean(Mockito.any(Class.class));

        initMocks();
    }

    private void initMocks() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(mockedAppender);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
        ((Logger) LoggerFactory.getLogger("it.gov.pagopa.bpd")).setLevel(Level.DEBUG);

        when(winningTransactionDaoMock.findPaymentToProcess(anyLong(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        transactions.add(TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType("00")
                                .build(), i, "setOperationType"));
                    }
                    return transactions;
                });
        when(winningTransactionDaoMock.findTotalTransferToProcess(anyLong(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        transactions.add(TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType("01")
                                .build(), i, "setOperationType"));
                    }
                    return transactions;
                });
        when(winningTransactionDaoMock.findPartialTranferToProcess(anyLong(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        transactions.add(TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType("01")
                                .build(), i, "setOperationType"));
                    }
                    return transactions;
                });

        when(aggregatorStrategy.aggregate(any(AwardPeriod.class), anyList()))
                .thenAnswer(invocationOnMock -> {
                    List<WinningTransaction> transactions = invocationOnMock.getArgument(1, List.class);
                    List<CitizenRanking> rankings = new ArrayList<>(transactions.size());
                    for (int i = 0; i < transactions.size(); i++) {
                        rankings.add(TestUtils.mockInstance(CitizenRanking.builder()
                                .fiscalCode(transactions.get(i).getFiscalCode())
                                .build(), i, "setFiscalCode"));
                    }
                    return rankings;
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

    protected PageRequest toPageable(SimplePageRequest pageRequest) {
        return PageRequest.of(pageRequest.getPage(), pageRequest.getSize());
    }

    @Test
    public void process_OK() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    protected abstract void verifyTrxToProcess(SimplePageRequest pageRequest, AwardPeriod awardPeriod);

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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();

        try {
            getCashbackUpdateService().process(awardPeriod, pageRequest);

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
            verifyTrxToProcess(pageRequest, awardPeriod);
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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();

        try {
            getCashbackUpdateService().process(awardPeriod, pageRequest);

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
            verifyTrxToProcess(pageRequest, awardPeriod);
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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();

        try {
            getCashbackUpdateService().process(awardPeriod, pageRequest);

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
            verifyTrxToProcess(pageRequest, awardPeriod);
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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();

        try {
            getCashbackUpdateService().process(awardPeriod, pageRequest);

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
            verifyTrxToProcess(pageRequest, awardPeriod);
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
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
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();

        try {
            getCashbackUpdateService().process(awardPeriod, pageRequest);

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
            verifyTrxToProcess(pageRequest, awardPeriod);
            BDDMockito.verify(citizenRankingDaoMock, times(1))
                    .updateCashback(anyList());
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