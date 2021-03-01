package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class PartialTransferAggregatorTest {

    protected final PartialTransferAggregator aggregator;
    protected final BigDecimal cashbackMultiplicand = BigDecimal.valueOf(0.1);

    public PartialTransferAggregatorTest(ExecutionStrategy executionStrategy) {
        ExecutionStrategyFactory executionStrategyFactoryMock = mock(ExecutionStrategyFactory.class);
        when(executionStrategyFactoryMock.create())
                .thenReturn(executionStrategy);
        aggregator = new PartialTransferAggregator(executionStrategyFactoryMock);
    }

    @Test
    public void aggregate_OkNothingToProcess() {
        AwardPeriod awardPeriod = getAwardPeriod();

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, Collections.emptyList());

        assertNotNull(rankings);
        assertEquals(0, rankings.size());
    }

    protected static AwardPeriod getAwardPeriod() {
        return AwardPeriod.builder()
                .awardPeriodId(1L)
                .maxTransactionEvaluated(150L)
                .cashbackPercentage(10)
                .build();
    }

    protected WinningTransaction createTrx(BigDecimal maxCashbackPerTrx, BigDecimal originalAmountBalance, BigDecimal amount, int bias) {
        return WinningTransaction.builder()
                .acquirerCode("acquirer_c")
                .correlationId("correlation_id_s_" + bias)
                .acquirerId("acquirer_id_s")
                .operationType("01")
                .trxDate(OffsetDateTime.now())
                .amountBalance(originalAmountBalance)
                .fiscalCode("fiscal_code_s_" + bias)
                .amount(amount)
                .score(amount.min(maxCashbackPerTrx)
                        .multiply(cashbackMultiplicand)
                        .negate()
                        .setScale(2, RoundingMode.HALF_DOWN))
                .build();
    }

    protected void fillExpectationMap(Map<String, Expectation> fiscalCode2ExpectationMap,
                                      int bias,
                                      BigDecimal maxCashbackPerTrx,
                                      BigDecimal origAmountBalance,
                                      BigDecimal updatedAmountBalance) {
        if (updatedAmountBalance.compareTo(maxCashbackPerTrx) < 0) {
            BigDecimal expectedBalance = origAmountBalance.min(maxCashbackPerTrx)
                    .subtract(updatedAmountBalance)
                    .multiply(cashbackMultiplicand)
                    .negate()
                    .setScale(2, RoundingMode.HALF_DOWN);
            long trxNumber = BigDecimal.ZERO.equals(updatedAmountBalance) ? -1L : 0L;
            fiscalCode2ExpectationMap.put("fiscal_code_s_" + bias, new Expectation(expectedBalance, trxNumber));
        }
    }

    @Data
    @AllArgsConstructor
    public static class Expectation {
        private BigDecimal totalCashback;
        private Long transactionNumber;
    }

}