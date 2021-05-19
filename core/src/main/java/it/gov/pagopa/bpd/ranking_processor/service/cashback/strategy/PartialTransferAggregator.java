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

import java.math.BigDecimal;
import java.time.LocalDate;
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
    private final LocalDate enableDate;


    @Autowired
    public PartialTransferAggregator(ExecutionStrategyFactory executionStrategyFactory,
                                     @Value(value = "${ranking-processor.enabledDate}")
                                     @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate enableDate) {
        executionStrategy = executionStrategyFactory.create();
        this.enableDate = enableDate;
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
        executionStrategy.streamSupplier(transactions).sequential()
                .peek(trx -> {
                    trx.setUpdateDate(now);
                    trx.setUpdateUser(RankingProcessorService.PROCESS_NAME);
                    if ((now.toLocalDate().isAfter(enableDate) || now.toLocalDate().equals(enableDate))) {
                        trx.setValid(trx.getOriginalAmountBalance().doubleValue() >= awardPeriod.getMinAmount().doubleValue());
                    }
                })
                .filter(trx -> trx.getValid() || trx.getValid() == null)
                .forEach(trx -> {
                    amountBalanceMap.compute(trx.getUniqueCorrelationKey(), (key, oldValue) -> {
                        BigDecimal oldAmountBalance = oldValue == null ? trx.getAmountBalance() : oldValue;
                        BigDecimal newAmountBalance = oldAmountBalance.subtract(trx.getAmount());
                        CitizenRanking.CitizenRankingBuilder rankingBuilder = CitizenRanking.builder()
                                .fiscalCode(trx.getFiscalCode())
                                .awardPeriodId(awardPeriod.getAwardPeriodId())
                                .updateDate(now)
                                .updateUser(RankingProcessorService.PROCESS_NAME);
                        if (newAmountBalance.compareTo(BigDecimal.ZERO) < 0) {
                            throw new IllegalStateException(String.format("Negative amount balance for transaction with idTrxAcquirer = %s, acquirerCode = %s, acquirerId = %s, trxDate = %s, correlationId = %s",
                                    trx.getIdTrxAcquirer(),
                                    trx.getAcquirerCode(),
                                    trx.getAcquirerId(),
                                    trx.getTrxDate(),
                                    trx.getCorrelationId()));
                        } else if (newAmountBalance.compareTo(BigDecimal.ZERO) == 0) {
                            BigDecimal actualScore = trx.getAmount()
                                    .multiply(negativeCashbackMultiplier)
                                    .setScale(2, ROUND_HALF_DOWN);
                            trx.setScore(actualScore);
                            rankingBuilder.totalCashback(actualScore)
                                    .transactionNumber(-1L);
                            rankingMap.merge(trx.getFiscalCode(), rankingBuilder.build(), CASHBACK_MAPPER);
                        } else if (newAmountBalance.compareTo(maxTrxEval) < 0) {
                            BigDecimal actualScore;
                            if (oldAmountBalance.compareTo(maxTrxEval) > 0) {
                                actualScore = maxTrxEval
                                        .subtract(newAmountBalance)
                                        .multiply(negativeCashbackMultiplier)
                                        .setScale(2, ROUND_HALF_DOWN);
                            } else {
                                actualScore = trx.getAmount()
                                        .multiply(negativeCashbackMultiplier)
                                        .setScale(2, ROUND_HALF_DOWN);
                            }
                            trx.setScore(actualScore);
                            rankingBuilder.transactionNumber(0L)
                                    .totalCashback(actualScore);
                            rankingMap.merge(trx.getFiscalCode(), rankingBuilder.build(), CASHBACK_MAPPER);
                        } else {
                            trx.setScore(BigDecimal.ZERO);
                        }
                        return newAmountBalance;
                    });
                });

        return rankingMap.values();
    }


}
