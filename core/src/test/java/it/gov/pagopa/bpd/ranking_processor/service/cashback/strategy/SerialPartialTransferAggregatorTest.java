package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerialPartialTransferAggregatorTest extends PartialTransferAggregatorTest {

    public SerialPartialTransferAggregatorTest() {
        super(new SerialExecutionStrategy());
    }


    @Test
    public void aggregate_OkWithReducedCashback() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        HashMap<String, Expectation> fiscalCode2ExpectationMap = new HashMap<>();
        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>();

        BigDecimal originalAmountBalance = BigDecimal.valueOf(200);
        int bias = 1;
        BigDecimal updatedAmountBalance = originalAmountBalance;
        for (int i = 0; i < 4; i++) {
            BigDecimal amount = BigDecimal.valueOf(20);
            WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, amount, bias);
            winningTransactions.add(trx);
            updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
        }
        fillExpectationMap(fiscalCode2ExpectationMap, bias, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

        originalAmountBalance = BigDecimal.valueOf(170);
        bias = 2;
        updatedAmountBalance = originalAmountBalance;
        for (int i = 0; i < 4; i++) {
            BigDecimal amount = BigDecimal.valueOf(5);
            WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, amount, bias);
            winningTransactions.add(trx);
            updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
        }
        fillExpectationMap(fiscalCode2ExpectationMap, bias, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

        originalAmountBalance = BigDecimal.valueOf(140);
        bias = 3;
        updatedAmountBalance = originalAmountBalance;
        for (int i = 0; i < 4; i++) {
            BigDecimal amount = BigDecimal.valueOf(5);
            WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, amount, bias);
            winningTransactions.add(trx);
            updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
        }
        fillExpectationMap(fiscalCode2ExpectationMap, bias, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

        Collection<CitizenRanking> rankings = aggregator.aggregate(awardPeriod, winningTransactions);

        assertNotNull(rankings);
        assertEquals(2, rankings.size());
        rankings.forEach(ranking -> {
            assertEquals(RankingProcessorService.PROCESS_NAME, ranking.getUpdateUser());
            assertNotNull(ranking.getUpdateDate());
            assertEquals(fiscalCode2ExpectationMap.get(ranking.getFiscalCode()).getTotalCashback(), ranking.getTotalCashback());
            assertEquals(fiscalCode2ExpectationMap.get(ranking.getFiscalCode()).getTransactionNumber(), ranking.getTransactionNumber());
        });
    }


    @Test
    public void aggregate_OkWithReducedCashbackAndTrxNumber() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>();
        HashMap<String, Expectation> fiscalCode2ExpectationMap = new HashMap<>();

        BigDecimal originalAmountBalance = BigDecimal.valueOf(200);
        int bias = 1;
        BigDecimal updatedAmountBalance = originalAmountBalance;
        for (int i = 0; i < 10; i++) {
            BigDecimal amount = BigDecimal.valueOf(20);
            WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, amount, bias);
            winningTransactions.add(trx);
            updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
        }
        fillExpectationMap(fiscalCode2ExpectationMap, bias, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

        originalAmountBalance = BigDecimal.valueOf(100);
        bias = 2;
        updatedAmountBalance = originalAmountBalance;
        for (int i = 0; i < 20; i++) {
            BigDecimal amount = BigDecimal.valueOf(5);
            WinningTransaction trx = createTrx(maxCashbackPerTrx, originalAmountBalance, amount, bias);
            winningTransactions.add(trx);
            updatedAmountBalance = updatedAmountBalance.subtract(trx.getAmount());
        }
        fillExpectationMap(fiscalCode2ExpectationMap, bias, maxCashbackPerTrx, originalAmountBalance, updatedAmountBalance);

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