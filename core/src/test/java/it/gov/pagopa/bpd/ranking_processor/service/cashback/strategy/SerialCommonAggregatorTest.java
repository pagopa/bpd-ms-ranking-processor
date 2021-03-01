package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerialCommonAggregatorTest extends CommonAggregatorTest {

    public SerialCommonAggregatorTest() {
        super(new SerialExecutionStrategy());
    }


    @Test
    public void aggregate_OkTotalTransferEdgeCases() {
        AwardPeriod awardPeriod = getAwardPeriod();
        BigDecimal maxCashbackPerTrx = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());

        HashMap<String, BigDecimal> fiscalCode2expectedTotalCashbackMap = new HashMap<>();

        ArrayList<WinningTransaction> winningTransactions = new ArrayList<>();
        WinningTransaction trx;
        int bias = 0;
        BigDecimal amount = BigDecimal.valueOf(200);
        trx = createTrx(maxCashbackPerTrx, null, amount, "01", bias);
        winningTransactions.add(trx);
        fiscalCode2expectedTotalCashbackMap.put(trx.getFiscalCode(), trx.getScore().setScale(2, RoundingMode.HALF_DOWN));

        bias++;
        amount = BigDecimal.valueOf(120);
        trx = createTrx(maxCashbackPerTrx, null, amount, "01", bias);
        winningTransactions.add(trx);
        fiscalCode2expectedTotalCashbackMap.put(trx.getFiscalCode(), trx.getScore().setScale(2, RoundingMode.HALF_DOWN));

        bias++;
        amount = BigDecimal.valueOf(80);
        trx = createTrx(maxCashbackPerTrx, null, amount, "01", bias);
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