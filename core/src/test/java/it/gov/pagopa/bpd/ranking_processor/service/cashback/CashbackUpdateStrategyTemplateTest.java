package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public abstract class CashbackUpdateStrategyTemplateTest {

    private static final int LIMIT = 5;
    private static final int LIMIT_WITH_WARN = 2;

    protected final WinningTransactionDao winningTransactionDaoMock;
    protected final CitizenRankingDao citizenRankingDaoMock;

    private final Appender mockedAppender;
    private final ArgumentCaptor<LoggingEvent> loggingEventCaptor;


    public CashbackUpdateStrategyTemplateTest() {
        this.winningTransactionDaoMock = Mockito.mock(WinningTransactionDao.class);
        this.citizenRankingDaoMock = Mockito.mock(CitizenRankingDao.class);
        this.loggingEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        this.mockedAppender = Mockito.mock(Appender.class);

        initMocks();
    }

    private void initMocks() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(mockedAppender);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
        ((Logger) LoggerFactory.getLogger("it.gov.pagopa.bpd")).setLevel(Level.DEBUG);

        when(winningTransactionDaoMock.findTransactionToProcess(anyLong(), any(WinningTransaction.TransactionType.class), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    WinningTransaction.TransactionType trxType = invocationOnMock.getArgument(1, WinningTransaction.TransactionType.class);
                    Pageable pageable = invocationOnMock.getArgument(2, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        transactions.add(TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType(WinningTransaction.TransactionType.PAYMENT.equals(trxType) ? "00" : "01")
                                .build(), i, "setOperationType"));
                    }
                    return transactions;
                });

        when(citizenRankingDaoMock.updateCashback(anyCollection()))
                .thenAnswer(invocationOnMock -> {
                    Collection argument = invocationOnMock.getArgument(0, Collection.class);
                    int size = argument.size();
                    if (argument.size() == LIMIT_WITH_WARN) {
                        size--;
                    }
                    return new int[size];
                });

        when(winningTransactionDaoMock.updateProcessedTransaction(anyCollection()))
                .thenAnswer(invocationOnMock -> {
                    Collection argument = invocationOnMock.getArgument(0, Collection.class);
                    int size = argument.size();
                    if (argument.size() == LIMIT_WITH_WARN) {
                        size--;
                    }
                    return new int[size];
                });
    }

    @Test
    public void process_OK() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        int processedTrxCount = getCashbackUpdateService().process(1L, WinningTransaction.TransactionType.PAYMENT, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
    }

    public abstract CashbackUpdateStrategy getCashbackUpdateService();

    @Test
    public void process_OK_withWarning() {
        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT_WITH_WARN);
        int processedTrxCount = getCashbackUpdateService().process(1L, WinningTransaction.TransactionType.PAYMENT, pageRequest);

        Assert.assertSame(LIMIT_WITH_WARN, processedTrxCount);
        int minNumberWarns = 2;
        verify(mockedAppender, atLeast(minNumberWarns)).doAppend(loggingEventCaptor.capture());
        for (LoggingEvent value : loggingEventCaptor.getAllValues()) {
            if (Level.WARN.equals(value.getLevel())) {
                if (String.format("updateCashback: affected %d rows of %d",
                        LIMIT_WITH_WARN - 1,
                        LIMIT_WITH_WARN).equals(value.getFormattedMessage())) {
                    minNumberWarns--;
                } else if (String.format("updateProcessedTransaction: affected %d rows of %d",
                        LIMIT_WITH_WARN - 1,
                        LIMIT_WITH_WARN).equals(value.getFormattedMessage())) {
                    minNumberWarns--;
                }
            }
        }
        Assert.assertEquals(0, minNumberWarns);
    }

}