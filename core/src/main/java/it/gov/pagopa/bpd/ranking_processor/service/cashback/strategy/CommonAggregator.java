package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy.CASHBACK_MAPPER;

/**
 * Common-case aggregator to handle payment and total transfer
 */
@Slf4j
@Service
class CommonAggregator implements AggregatorStrategy {

    private final ExecutionStrategy executionStrategy;
    private final LocalDate enableDate;


    @Autowired
    public CommonAggregator(ExecutionStrategyFactory executionStrategyFactory,
                            @Value(value = "${ranking-processor.enabledDate}")
                            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate enableDate) {
        executionStrategy = executionStrategyFactory.create();
        this.enableDate = enableDate;
    }

    @Override
    public Collection<CitizenRanking> aggregate(AwardPeriod awardPeriod, List<WinningTransaction> transactions) {
        OffsetDateTime now = OffsetDateTime.now();
        Map<String, CitizenRanking> cashbackMap = executionStrategy.streamSupplier(transactions)
                .peek(trx -> {
                    trx.setUpdateDate(now);
                    trx.setUpdateUser(RankingProcessorService.PROCESS_NAME);
                    if ((now.toLocalDate().isAfter(enableDate) || now.toLocalDate().equals(enableDate))) {
                        trx.setValid(trx.getAmount().longValue() > awardPeriod.getMinAmount().longValue());
                    }
                })
                .filter(trx -> trx.getValid() || trx.getValid() == null)
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
