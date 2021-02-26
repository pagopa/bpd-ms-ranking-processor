package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategy;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy.CashbackUpdateStrategyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

/**
 * {@link RankingSubProcessCommand} implementation for Update Cashback subprocess
 */
@Slf4j
@Service
@Order(1)
class UpdateCashbackCommandImpl implements RankingSubProcessCommand {

    private final CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory;
    private final int cashbackUpdateLimit;

    @Autowired
    public UpdateCashbackCommandImpl(CashbackUpdateStrategyFactory cashbackUpdateStrategyFactory,
                                     @Value("${cashback-update.data-extraction.limit}") int cashbackUpdateLimit) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommandImpl.UpdateCashbackCommandImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("cashbackUpdateStrategyFactory = {}, cashbackUpdateLimit = {}", cashbackUpdateStrategyFactory, cashbackUpdateLimit);
        }

        this.cashbackUpdateStrategyFactory = cashbackUpdateStrategyFactory;
        this.cashbackUpdateLimit = cashbackUpdateLimit;
    }

    @Override
    public void execute(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommandImpl.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        for (TransactionType trxType : TransactionType.values()) {
            CashbackUpdateStrategy cashbackUpdateStrategy = getCashbackUpdateStrategy(trxType);
            int trxCount;
            do {
                SimplePageRequest pageRequest = SimplePageRequest.of(0, cashbackUpdateLimit);
                trxCount = cashbackUpdateStrategy.process(awardPeriod, trxType, pageRequest);
            } while (trxCount == cashbackUpdateLimit);
        }
    }

    public CashbackUpdateStrategy getCashbackUpdateStrategy(TransactionType trxType) {
        return cashbackUpdateStrategyFactory.create(trxType);
    }

}
