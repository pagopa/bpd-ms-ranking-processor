package it.gov.pagopa.bpd.ranking_processor.service;


import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = CashbackProcessingServiceImpl.class)
@TestPropertySource(properties = {
        "ranking-processor-cashback-processing.data-extraction.limit=10"
})
public class CashbackProcessingServiceImplTest {

    @MockBean
    private WinningTransactionDao winningTransactionDao;
    @MockBean
    private CitizenRankingDao citizenRankingDao;
    @Autowired
    private CashbackProcessingServiceImpl cashbackProcessingService;

    @PostConstruct
    public void initMocks() {
        when(winningTransactionDao.findTransactionToProcess(anyLong(), any(TransactionType.class), any(Pageable.class)))
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
    }

    @Test
    public void processCashbackOK() {
        when(citizenRankingDao.updateCashback(anyCollection()))
                .thenAnswer(invocationOnMock -> new int[invocationOnMock.getArgument(0, Collection.class).size()]);
        when(winningTransactionDao.updateProcessedTransaction(anyCollection()))
                .thenAnswer(invocationOnMock -> new int[invocationOnMock.getArgument(0, Collection.class).size()]);

        int pageSize = 10;
        int processedTrxCount = cashbackProcessingService.processCashback(1L,
                TransactionType.PAYMENT,
                PageRequest.of(0, pageSize));

        Assert.assertSame(pageSize, processedTrxCount);
    }

    @Test
    public void processCashbackOK_withWarning() {
        when(citizenRankingDao.updateCashback(anyCollection()))
                .thenAnswer(invocationOnMock -> new int[invocationOnMock.getArgument(0, Collection.class).size() - 1]);
        when(winningTransactionDao.updateProcessedTransaction(anyCollection()))
                .thenAnswer(invocationOnMock -> new int[invocationOnMock.getArgument(0, Collection.class).size() - 1]);

        int pageSize = 10;
        int processedTrxCount = cashbackProcessingService.processCashback(1L,
                TransactionType.PAYMENT,
                PageRequest.of(0, pageSize));

        Assert.assertSame(pageSize, processedTrxCount);
    }

}