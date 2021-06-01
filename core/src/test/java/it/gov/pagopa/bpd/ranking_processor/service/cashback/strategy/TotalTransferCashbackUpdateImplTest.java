package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TotalTransferCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    private static boolean correlationId;
    private static boolean matchPayment;
    private static boolean paymentWithSameAmount;
    private static boolean retention;

    public TotalTransferCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new TotalTransferCashbackUpdate(winningTransactionDaoMock, citizenRankingDaoMock, beanFactoryMock, 2, Period.parse("P1D"));
    }

    @Override
    protected void verifyTrxToProcess(SimplePageRequest pageRequest, AwardPeriod awardPeriod) {
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findTransferToProcess(any(), eq(toPageable(pageRequest)));
    }

    @Override
    public CashbackUpdateStrategy getCashbackUpdateService() {
        return cashbackUpdateStrategy;
    }

    @Override
    protected PageRequest toPageable(SimplePageRequest pageRequest) {
        return PageRequest.of(pageRequest.getPage(), pageRequest.getSize(), Sort.by("fiscal_code_s"));
    }

    @Override
    public void init() {
        super.init();
        correlationId = false;
        matchPayment = false;
        paymentWithSameAmount = false;
        retention = false;
    }

    @Override
    protected void initMocks() {
        super.initMocks();
        when(winningTransactionDaoMock.findTransferToProcess(any(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        String[] ignoredSetters;
                        if (correlationId) {
                            ignoredSetters = new String[]{"setOperationType", "setAmount"};
                        } else {
                            ignoredSetters = new String[]{"setOperationType", "setAmount", "setCorrelationId"};
                        }
                        WinningTransaction trx = TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType("01")
                                .amount(BigDecimal.ONE)
                                .build(), i, ignoredSetters);
                        if (retention) {
                            trx.setInsertDate(OffsetDateTime.now().minus(Period.ofMonths(1)));
                        }
                        transactions.add(trx);
                    }
                    return transactions;
                });
        when(winningTransactionDaoMock.findPaymentTrxWithCorrelationId(any()))
                .thenAnswer(invocationOnMock -> {
                    if (matchPayment) {
                        if (paymentWithSameAmount) {
                            return TestUtils.mockInstance(WinningTransaction.builder()
                                    .operationType("00")
                                    .amount(BigDecimal.ONE)
                                    .build(), "setOperationType", "setAmount");
                        } else {
                            return TestUtils.mockInstance(WinningTransaction.builder()
                                    .operationType("00")
                                    .amount(BigDecimal.valueOf(-1))
                                    .build(), "setOperationType", "setAmount");
                        }
                    } else {
                        return null;
                    }
                });
        when(winningTransactionDaoMock.findPaymentTrxWithoutCorrelationId(any()))
                .thenAnswer(invocationOnMock -> {
                    if (matchPayment) {
                        if (paymentWithSameAmount) {
                            return TestUtils.mockInstance(WinningTransaction.builder()
                                    .operationType("00")
                                    .amount(BigDecimal.ONE)
                                    .build(), "setOperationType", "setAmount");
                        } else {
                            return TestUtils.mockInstance(WinningTransaction.builder()
                                    .operationType("00")
                                    .amount(BigDecimal.valueOf(-1))
                                    .build(), "setOperationType", "setAmount");
                        }
                    } else {
                        return null;
                    }
                });
        when(winningTransactionDaoMock.updateUnrelatedTransfer(any()))
                .thenAnswer(invocationOnMock -> {
                    Collection transactions = invocationOnMock.getArgument(0, Collection.class);
                    if (!matchPayment) {
                        int[] result = new int[transactions.size()];
                        Arrays.fill(result, 1);
                        return result;
                    } else {
                        return null;
                    }
                });
        when(winningTransactionDaoMock.updateUnprocessedPartialTransfer(any()))
                .thenAnswer(invocationOnMock -> {
                    Collection transactions = invocationOnMock.getArgument(0, Collection.class);
                    if (!paymentWithSameAmount) {
                        int[] result = new int[transactions.size()];
                        Arrays.fill(result, 1);
                        return result;
                    } else {
                        return null;
                    }
                });
        when(winningTransactionDaoMock.deleteTransfer(any()))
                .thenAnswer(invocationOnMock -> {
                    Collection transactions = invocationOnMock.getArgument(0, Collection.class);
                    if (!paymentWithSameAmount) {
                        int[] result = new int[transactions.size()];
                        Arrays.fill(result, 1);
                        return result;
                    } else {
                        return null;
                    }
                });
    }


    @Override
    public void process_OK() {
        // see specific process_OK tests
    }

    @Test
    public void process_OK_WithCorrIdAndNoMatchPayment() {
        correlationId = true;
        matchPayment = false;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateUnrelatedTransfer(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, never())
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test
    public void process_OK_WithoutCorrIdAndNoMatchPayment() {
        correlationId = false;
        matchPayment = false;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithoutCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateUnrelatedTransfer(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateUnprocessedPartialTransfer(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, never())
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test
    public void process_OK_WithCorrIdAndMatchPaymentWithDifferentAmount() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateUnprocessedPartialTransfer(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateUnrelatedTransfer(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, never())
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test
    public void process_OK_WithoutCorrIdAndMatchPayment() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithoutCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateUnprocessedPartialTransfer(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, never())
                .updateUnrelatedTransfer(anyCollection());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test
    public void process_OK_WithRetention() {
        retention = true;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .deleteTransfer(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Override
    public void process_OK_updateCashbackMiss() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_OK_updateCashbackMiss();
    }

    @Override
    public void process_KO_updateCashbackError() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_KO_updateCashbackError();
    }

    @Override
    public void process_KO_insertCashbackError() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_KO_insertCashbackError();
    }

    @Override
    public void process_KO_insertCashbackMiss() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_KO_insertCashbackMiss();
    }

    @Override
    public void process_KO_updateTransactionError() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_KO_updateTransactionError();
    }

    @Override
    public void process_KO_updateTransactionMissing() {
        correlationId = false;
        matchPayment = true;
        paymentWithSameAmount = true;

        super.process_KO_updateTransactionMissing();
    }

    @Override
    protected void verifyExtraConditions() {
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithoutCorrelationId(any());
    }
}