package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
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
    public void execute(long awardPeriodId) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateCashbackCommandImpl.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }

        CashbackUpdateStrategy cashbackUpdateStrategy = getCashbackUpdateStrategy();
        for (WinningTransaction.TransactionType trxType : WinningTransaction.TransactionType.values()) {
            int trxCount;
            do {
                SimplePageRequest pageRequest = SimplePageRequest.of(0, cashbackUpdateLimit);
                trxCount = cashbackUpdateStrategy.process(awardPeriodId, trxType, pageRequest);
            } while (cashbackUpdateLimit == trxCount);
        }
    }

    public CashbackUpdateStrategy getCashbackUpdateStrategy() {
        return cashbackUpdateStrategyFactory.create();
    }

}
