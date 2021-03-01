package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommonAggregatorTest {

    private final CommonAggregator aggregator;


    public CommonAggregatorTest() {
        ExecutionStrategyFactory executionStrategyFactoryMock = mock(ExecutionStrategyFactory.class);
        when(executionStrategyFactoryMock.create())
                .thenReturn(new ParallelExecutionStrategy());
        aggregator = new CommonAggregator(executionStrategyFactoryMock);
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

    @Test
    public void aggregate_OkWithReducedCashbackAndTrxNumber() {
        AwardPeriod awardPeriod = getAwardPeriod();

        HashMap<String, BigDecimal> fiscalCode2expectedTotalCashbackMap = new HashMap<>();

        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>();
        WinningTransaction trx;
        trx = WinningTransaction.builder()
                .acquirerCode("acquirer_c")
                .trxDate(OffsetDateTime.now())
                .correlationId("correlation_id_s_1")
                .acquirerId("acquirer_id_s")
                .fiscalCode("fiscal_code_s_1")
                .operationType("01")
                .amount(BigDecimal.valueOf(200))
                .score(BigDecimal.valueOf(-15))
                .build();
        winningTransactions.add(trx);
        fiscalCode2expectedTotalCashbackMap.put(trx.getFiscalCode(), trx.getScore().setScale(2, RoundingMode.HALF_DOWN));
        trx = WinningTransaction.builder()
                .acquirerCode("acquirer_c")
                .trxDate(OffsetDateTime.now())
                .correlationId("correlation_id_s_2")
                .acquirerId("acquirer_id_s")
                .fiscalCode("fiscal_code_s_2")
                .operationType("01")
                .amount(BigDecimal.valueOf(120))
                .score(BigDecimal.valueOf(-12.5))
                .build();
        winningTransactions.add(trx);
        fiscalCode2expectedTotalCashbackMap.put(trx.getFiscalCode(), trx.getScore().setScale(2, RoundingMode.HALF_DOWN));
        trx = WinningTransaction.builder()
                .acquirerCode("acquirer_c")
                .trxDate(OffsetDateTime.now())
                .correlationId("correlation_id_s_3")
                .acquirerId("acquirer_id_s")
                .fiscalCode("fiscal_code_s_3")
                .operationType("01")
                .amount(BigDecimal.valueOf(80))
                .score(BigDecimal.valueOf(-0.8))
                .build();
        winningTransactions.add(trx);
        fiscalCode2expectedTotalCashbackMap.put(trx.getFiscalCode(), trx.getScore().setScale(2, RoundingMode.HALF_DOWN));

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, winningTransactions);

        assertNotNull(rankings);
        assertEquals(3, rankings.size());
        rankings.forEach(citizenRanking -> {
            assertEquals(Long.valueOf(-1), citizenRanking.getTransactionNumber());
            assertEquals(RankingProcessorService.PROCESS_NAME, citizenRanking.getUpdateUser());
            assertNotNull(citizenRanking.getUpdateDate());
            assertEquals(fiscalCode2expectedTotalCashbackMap.get(citizenRanking.getFiscalCode()), citizenRanking.getTotalCashback());
        });
    }

}