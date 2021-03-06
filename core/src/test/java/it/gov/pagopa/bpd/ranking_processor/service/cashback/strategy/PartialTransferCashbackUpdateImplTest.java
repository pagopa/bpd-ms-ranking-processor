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

public class PartialTransferCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    private static boolean correlationId;
    private static boolean matchPayment;
    private static boolean paymentWithSameAmount;
    private static boolean retention;
    private static boolean cashbackError;
    private static boolean negativeAmountBalance;


    public PartialTransferCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new PartialTransferCashbackUpdate(winningTransactionDaoMock, citizenRankingDaoMock, beanFactoryMock, 2, Period.parse("P1D"));
    }


    @Override
    protected void verifyTrxToProcess(SimplePageRequest pageRequest, AwardPeriod awardPeriod) {
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findPartialTransferToProcess(any(), eq(toPageable(pageRequest)));
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
        cashbackError = false;
        negativeAmountBalance = false;
    }


    @Override
    protected void initMocks() {
        super.initMocks();
        when(winningTransactionDaoMock.findPartialTransferToProcess(any(), any(Pageable.class)))
                .thenAnswer(invocationOnMock -> {
                    Pageable pageable = invocationOnMock.getArgument(1, Pageable.class);
                    List<WinningTransaction> transactions = new ArrayList<>(pageable.getPageSize());
                    for (int i = 0; i < pageable.getPageSize(); i++) {
                        String[] ignoredSetters;
                        if (correlationId) {
                            ignoredSetters = new String[]{"setOperationType", "setAmount", "setParked"};
                        } else {
                            ignoredSetters = new String[]{"setOperationType", "setAmount", "setParked", "setCorrelationId"};
                        }
                        WinningTransaction trx = TestUtils.mockInstance(WinningTransaction.builder()
                                .operationType("01")
                                .amount(BigDecimal.ONE)
                                .build(), i, ignoredSetters);
                        if (retention) {
                            trx.setInsertDate(OffsetDateTime.now().minus(Period.ofMonths(1)));
                        }
                        if (negativeAmountBalance) {
                            trx.setParked(true);
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
                                    .build(), "setOperationType", "setAmount", "setParked");
                        } else {
                            return TestUtils.mockInstance(WinningTransaction.builder()
                                    .operationType("00")
                                    .amount(BigDecimal.TEN)
                                    .build(), "setOperationType", "setAmount", "setParked");
                        }
                    } else {
                        return null;
                    }
                });
        when(winningTransactionDaoMock.findProcessedTransferAmount(any()))
                .thenReturn(BigDecimal.ONE);
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
                    if (negativeAmountBalance) {
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
                    int[] result = new int[transactions.size()];
                    Arrays.fill(result, 1);
                    return result;
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
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Test
    public void process_OK_WithoutCorrId() {
        correlationId = false;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateUnrelatedTransfer(anyCollection());
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
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findProcessedTransferAmount(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateProcessedTransaction(anyCollection());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .deleteTransfer(anyList());
        BDDMockito.verify(citizenRankingDaoMock, times(1))
                .updateCashback(anyList());
        verifyNoMoreInteractions(winningTransactionDaoMock, citizenRankingDaoMock);
    }


    @Test
    public void process_OK_WithCorrIdAndMatchPaymentWithDifferentAmountAndNegativeAmountBalance() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;
        negativeAmountBalance = true;

        SimplePageRequest pageRequest = SimplePageRequest.of(0, LIMIT);
        AwardPeriod awardPeriod = AwardPeriod.builder()
                .awardPeriodId(1L)
                .build();
        int processedTrxCount = getCashbackUpdateService().process(awardPeriod, pageRequest);

        Assert.assertSame(LIMIT, processedTrxCount);
        verifyTrxToProcess(pageRequest, awardPeriod);
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findProcessedTransferAmount(any());
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .updateUnprocessedPartialTransfer(anyCollection());
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
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;

        super.process_OK_updateCashbackMiss();
    }

    @Override
    public void process_KO_updateCashbackError() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;
        cashbackError = true;

        super.process_KO_updateCashbackError();
    }

    @Override
    public void process_KO_insertCashbackError() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;
        cashbackError = true;

        super.process_KO_insertCashbackError();
    }

    @Override
    public void process_KO_insertCashbackMiss() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;
        cashbackError = true;

        super.process_KO_insertCashbackMiss();
    }

    @Override
    public void process_KO_updateTransactionError() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;

        super.process_KO_updateTransactionError();
    }

    @Override
    public void process_KO_updateTransactionMissing() {
        correlationId = true;
        matchPayment = true;
        paymentWithSameAmount = false;

        super.process_KO_updateTransactionMissing();
    }

    @Override
    protected void verifyExtraConditions() {
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findPaymentTrxWithCorrelationId(any());
        BDDMockito.verify(winningTransactionDaoMock, times(LIMIT))
                .findProcessedTransferAmount(any());
        if (!cashbackError) {
            BDDMockito.verify(winningTransactionDaoMock, times(1))
                    .deleteTransfer(anyList());
        }
    }

}