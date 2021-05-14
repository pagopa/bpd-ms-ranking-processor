package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Map;

public abstract class PartialTransferAggregatorTest extends AggregatorTest {


    public PartialTransferAggregatorTest(ExecutionStrategy executionStrategy) {
        super(executionStrategy);
    }

    @Override
    protected AggregatorStrategy getAggregatorInstance(ExecutionStrategyFactory executionStrategyFactoryMock, LocalDate localDate) {
        return new PartialTransferAggregator(executionStrategyFactoryMock, getEnabledDate());
    }

    protected void fillExpectationMap(Map<String, Expectation> fiscalCode2ExpectationMap,
                                      int bias,
                                      BigDecimal maxCashbackPerTrx,
                                      BigDecimal origAmountBalance,
                                      BigDecimal updatedAmountBalance) {
        if (updatedAmountBalance.compareTo(maxCashbackPerTrx) < 0) {
            BigDecimal expectedBalance = origAmountBalance.min(maxCashbackPerTrx)
                    .subtract(updatedAmountBalance)
                    .multiply(CASHBACK_MULTIPLICAND)
                    .negate()
                    .setScale(2, RoundingMode.HALF_DOWN);
            long trxNumber = BigDecimal.ZERO.equals(updatedAmountBalance) ? -1L : 0L;
            fiscalCode2ExpectationMap.put("fiscal_code_s_" + bias, new Expectation(expectedBalance, trxNumber));
        }
    }

}