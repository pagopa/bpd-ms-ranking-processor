package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AggregatorTest {

    protected static final BigDecimal CASHBACK_MULTIPLICAND = BigDecimal.valueOf(0.1);

    protected final AggregatorStrategy aggregator;


    public AggregatorTest(ExecutionStrategy executionStrategy) {
        ExecutionStrategyFactory executionStrategyFactoryMock = mock(ExecutionStrategyFactory.class);
        when(executionStrategyFactoryMock.create())
                .thenReturn(executionStrategy);
        aggregator = getAggregatorInstance(executionStrategyFactoryMock, getEnabledDate());
    }


    protected abstract AggregatorStrategy getAggregatorInstance(ExecutionStrategyFactory executionStrategyFactoryMock, LocalDate localDate);

    protected static WinningTransaction createTrx(BigDecimal maxCashbackPerTrx, BigDecimal originalAmountBalance, BigDecimal amount, String operationType, int bias) {
        BigDecimal score = amount.min(maxCashbackPerTrx)
                .multiply(CASHBACK_MULTIPLICAND)
                .setScale(2, RoundingMode.HALF_DOWN);
        return WinningTransaction.builder()
                .acquirerCode("acquirer_c")
                .correlationId("correlation_id_s_" + bias)
                .acquirerId("acquirer_id_s")
                .operationType(operationType)
                .trxDate(OffsetDateTime.now())
                .amountBalance(originalAmountBalance)
                .fiscalCode("fiscal_code_s_" + bias)
                .amount(amount)
                .score("01".equals(operationType) ? score.negate() : score)
                .originalAmountBalance(originalAmountBalance)
                .valid(true)
                .build();
    }

    @Test
    public void aggregate_OkNothingToProcess() {
        AwardPeriod awardPeriod = getAwardPeriod();

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, Collections.emptyList());

        assertNotNull(rankings);
        assertEquals(0, rankings.size());
    }

    protected static LocalDate getEnabledDate() {
        return LocalDate.now();
    }

    protected static AwardPeriod getAwardPeriod() {
        return AwardPeriod.builder()
                .awardPeriodId(1L)
                .maxTransactionEvaluated(150L)
                .cashbackPercentage(10)
                .minAmount(BigDecimal.valueOf(1))
                .build();
    }


    @Data
    @AllArgsConstructor
    public static class Expectation {
        private BigDecimal totalCashback;
        private Long transactionNumber;
    }

}