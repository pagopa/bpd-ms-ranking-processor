package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ParallelPartialTransferAggregatorTest extends PartialTransferAggregatorTest {

    public ParallelPartialTransferAggregatorTest() {
        super(new ParallelExecutionStrategy());
    }

    @Test
    public void aggregate_OkWithReducedCashback() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        Map<String, Expectation> fiscalCode2ExpectationMap = new HashMap<>();

        Random random = new Random();

        int groupLoop;
        do {
            groupLoop = random.nextInt(1000);
        } while (groupLoop == 0);
        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>(groupLoop * 10);
        for (int i = 0; i < groupLoop; i++) {
            double origBal;
            do {
                origBal = Math.random() * 1000;
            } while (origBal < 1);
            BigDecimal originalAmountBalance = BigDecimal.valueOf(origBal);
            BigDecimal updatedAmountBalance = originalAmountBalance;
            int trxLoop;
            do {
                trxLoop = random.nextInt(10);
            } while (trxLoop == 0);
            trxLoop = Math.min(trxLoop, (int) origBal);
            for (int j = 0; j < trxLoop; j++) {
                WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, BigDecimal.valueOf(1), "01", i);
                winningTransactions.add(trx);
                updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
            }
            fillExpectationMap(fiscalCode2ExpectationMap, i, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

        }

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, winningTransactions);

        assertNotNull(rankings);
        assertEquals(fiscalCode2ExpectationMap.size(), rankings.size());
        rankings.forEach(ranking -> {
            assertEquals(RankingProcessorService.PROCESS_NAME, ranking.getUpdateUser());
            assertNotNull(ranking.getUpdateDate());
            assertEquals(fiscalCode2ExpectationMap.get(ranking.getFiscalCode()).getTotalCashback(), ranking.getTotalCashback());
            assertEquals(fiscalCode2ExpectationMap.get(ranking.getFiscalCode()).getTransactionNumber(), ranking.getTransactionNumber());
        });
    }

}
