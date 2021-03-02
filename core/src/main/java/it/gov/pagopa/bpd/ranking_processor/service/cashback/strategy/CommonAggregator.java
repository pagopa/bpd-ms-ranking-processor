package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy.CASHBACK_MAPPER;

/**
 * Standard aggregator to handle payment and total transfer
 */
@Slf4j
@Service
class CommonAggregator implements AggregatorStrategy {

    private final ExecutionStrategy executionStrategy;

    @Autowired
    public CommonAggregator(ExecutionStrategyFactory executionStrategyFactory) {
        executionStrategy = executionStrategyFactory.create();
    }

    @Override
    public Collection<CitizenRanking> aggregate(AwardPeriod awardPeriod, List<WinningTransaction> transactions) {
        OffsetDateTime now = OffsetDateTime.now();
        Map<String, CitizenRanking> cashbackMap = executionStrategy.streamSupplier(transactions)
                .peek(trx -> {
                    trx.setUpdateDate(now);
                    trx.setUpdateUser(RankingProcessorService.PROCESS_NAME);
                })
                .map(trx -> CitizenRanking.builder()
                        .fiscalCode(trx.getFiscalCode())
                        .awardPeriodId(awardPeriod.getAwardPeriodId())
                        .totalCashback(trx.getScore().setScale(2, RoundingMode.HALF_DOWN))
                        .transactionNumber("01".equals(trx.getOperationType()) ? -1L : 1L)
                        .updateDate(now)
                        .updateUser(RankingProcessorService.PROCESS_NAME)
                        .build())
                .collect(executionStrategy.toMap(CitizenRanking::getFiscalCode, Function.identity(), CASHBACK_MAPPER));

        return cashbackMap.values();
    }

}
