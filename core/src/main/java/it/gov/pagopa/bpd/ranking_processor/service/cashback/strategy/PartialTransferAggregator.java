package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy.CASHBACK_MAPPER;
import static java.math.BigDecimal.ROUND_HALF_DOWN;

/**
 * Aggregator to handle partial transfer
 */
@Slf4j
@Service
class PartialTransferAggregator implements AggregatorStrategy {

    private final ExecutionStrategy executionStrategy;

    @Autowired
    public PartialTransferAggregator(ExecutionStrategyFactory executionStrategyFactory) {
        executionStrategy = executionStrategyFactory.create();
    }


    @Override
    public Collection<CitizenRanking> aggregate(AwardPeriod awardPeriod, List<WinningTransaction> transactions) {
        if (log.isTraceEnabled()) {
            log.trace("PartialTransferCashbackUpdate.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriod = {}, transactions = {}", awardPeriod, transactions);
        }

        Map<String, CitizenRanking> rankingMap = executionStrategy.unorderedMapSupplier();
        Map<String, BigDecimal> amountBalanceMap = executionStrategy.unorderedMapSupplier();
        OffsetDateTime now = OffsetDateTime.now();
        BigDecimal maxTrxEval = BigDecimal.valueOf(awardPeriod.getMaxTransactionEvaluated());
        BigDecimal negativeCashbackMultiplier = BigDecimal.valueOf(awardPeriod.getCashbackPercentage() / -100.0);
        executionStrategy.streamSupplier(transactions).forEach(trx -> {
            trx.setUpdateDate(now);
            trx.setUpdateUser(RankingProcessorService.PROCESS_NAME);
            amountBalanceMap.compute(trx.getUniqueCorrelationKey(), (key, oldValue) -> {
                BigDecimal oldAmountBalance = oldValue == null ? trx.getAmountBalance() : oldValue;
                BigDecimal newAmountBalance = oldAmountBalance.subtract(trx.getAmount());
                CitizenRanking.CitizenRankingBuilder rankingBuilder = CitizenRanking.builder()
                        .fiscalCode(trx.getFiscalCode())
                        .awardPeriodId(awardPeriod.getAwardPeriodId())
                        .updateDate(now)
                        .updateUser(RankingProcessorService.PROCESS_NAME);
                if (newAmountBalance.compareTo(BigDecimal.ZERO) < 0) {
                    throw new IllegalStateException(String.format("Negative amount balance for transaction with idTrxAcquirer = %s, acquirerCode = %s, acquirerId = %s, correlationId = %s",
                            trx.getIdTrxAcquirer(),
                            trx.getAcquirerCode(),
                            trx.getAcquirerId(),
                            trx.getCorrelationId()));
                } else if (newAmountBalance.compareTo(BigDecimal.ZERO) == 0) {
                    rankingBuilder
                            .totalCashback(trx.getScore().setScale(2, RoundingMode.HALF_DOWN))
                            .transactionNumber(-1L);
                    rankingMap.merge(trx.getFiscalCode(), rankingBuilder.build(), CASHBACK_MAPPER);
                } else if (newAmountBalance.compareTo(maxTrxEval) < 0) {
                    rankingBuilder.transactionNumber(0L);
                    if (oldAmountBalance.compareTo(maxTrxEval) > 0) {
                        rankingBuilder.totalCashback(maxTrxEval
                                .subtract(newAmountBalance)
                                .multiply(negativeCashbackMultiplier)
                                .setScale(2, ROUND_HALF_DOWN));
                    } else {
                        rankingBuilder.totalCashback(trx.getScore().setScale(2, RoundingMode.HALF_DOWN));
                    }
                    rankingMap.merge(trx.getFiscalCode(), rankingBuilder.build(), CASHBACK_MAPPER);
                }
                return newAmountBalance;
            });
        });

        return rankingMap.values();
    }

}
