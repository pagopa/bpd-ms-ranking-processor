package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.*;

public abstract class CommonAggregatorTest extends AggregatorTest {

    public CommonAggregatorTest(ExecutionStrategy executionStrategy) {
        super(new SerialExecutionStrategy());
    }

    @Override
    protected AggregatorStrategy getAggregatorInstance(ExecutionStrategyFactory executionStrategyFactoryMock) {
        return new CommonAggregator(executionStrategyFactoryMock);
    }


    @Test
    public void aggregate_OkTotalTransfer() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        HashMap<String, Expectation> fiscalCode2ExpectationMap = new HashMap<>();
        List<WinningTransaction> winningTransactions = buildTransactions(fiscalCode2ExpectationMap, maxCashbackPerTrx, "01");

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, winningTransactions);

        assertNotNull(rankings);
        assertEquals(fiscalCode2ExpectationMap.size(), rankings.size());
        rankings.forEach(citizenRanking -> {
            assertEquals(RankingProcessorService.PROCESS_NAME, citizenRanking.getUpdateUser());
            assertNotNull(citizenRanking.getUpdateDate());
            assertTrue(citizenRanking.getTotalCashback().compareTo(BigDecimal.ZERO) < 0);
            assertTrue(citizenRanking.getTransactionNumber() < 0);
            assertEquals(fiscalCode2ExpectationMap.get(citizenRanking.getFiscalCode()).getTotalCashback(), citizenRanking.getTotalCashback());
            assertEquals(fiscalCode2ExpectationMap.get(citizenRanking.getFiscalCode()).getTransactionNumber(), citizenRanking.getTransactionNumber());
        });
    }


    private ArrayList<WinningTransaction> buildTransactions(HashMap<String, Expectation> fiscalCode2ExpectationMap,
                                                            BigDecimal maxCashbackPerTrx,
                                                            String operationType) {
        Random random = new Random();
        int groupLoop;
        do {
            groupLoop = random.nextInt(100);
        } while (groupLoop == 0);
        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>(groupLoop * 10);
        for (int i = 0; i < groupLoop; i++) {
            double amountRand;
            do {
                amountRand = Math.random() * 1000;
            } while (amountRand < 1);
            int trxLoop;
            do {
                trxLoop = random.nextInt(10);
            } while (trxLoop == 0);
            trxLoop = Math.min(trxLoop, (int) amountRand);
            BigDecimal amount = BigDecimal.valueOf(amountRand);
            for (int j = 0; j < trxLoop; j++) {
                WinningTransaction trx = createTrx(maxCashbackPerTrx, null, amount, operationType, i);
                winningTransactions.add(trx);
                fillExpectationMap(fiscalCode2ExpectationMap, i, trx.getScore());
            }
        }
        return winningTransactions;
    }

    private void fillExpectationMap(HashMap<String, Expectation> fiscalCode2ExpectationMap, int bias, BigDecimal score) {
        long trxNumber = score.signum() < 0 ? -1L : 1L;
        fiscalCode2ExpectationMap.merge("fiscal_code_s_" + bias, new Expectation(score, trxNumber), (v1, v2) -> {
            BigDecimal totCashback = v1.getTotalCashback().add(v2.getTotalCashback());
            long trxNum = v1.getTransactionNumber() + v2.getTransactionNumber();
            return new Expectation(totCashback, trxNum);
        });
    }

    @Test
    public void aggregate_OkPayment() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        HashMap<String, Expectation> fiscalCode2ExpectationMap = new HashMap<>();
        List<WinningTransaction> winningTransactions = buildTransactions(fiscalCode2ExpectationMap, maxCashbackPerTrx, "00");

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, winningTransactions);

        assertNotNull(rankings);
        assertEquals(fiscalCode2ExpectationMap.size(), rankings.size());
        rankings.forEach(citizenRanking -> {
            assertEquals(RankingProcessorService.PROCESS_NAME, citizenRanking.getUpdateUser());
            assertNotNull(citizenRanking.getUpdateDate());
            assertTrue(citizenRanking.getTotalCashback().compareTo(BigDecimal.ZERO) > 0);
            assertTrue(citizenRanking.getTransactionNumber() > 0);
            assertEquals(fiscalCode2ExpectationMap.get(citizenRanking.getFiscalCode()).getTotalCashback(), citizenRanking.getTotalCashback());
            assertEquals(fiscalCode2ExpectationMap.get(citizenRanking.getFiscalCode()).getTransactionNumber(), citizenRanking.getTransactionNumber());
        });
    }

}