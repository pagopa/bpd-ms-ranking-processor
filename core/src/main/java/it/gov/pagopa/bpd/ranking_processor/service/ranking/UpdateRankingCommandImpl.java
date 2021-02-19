package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingSubProcessCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

/**
 * {@link RankingSubProcessCommand} implementation for Update Ranking subprocess
 */
@Slf4j
@Service
@Order(2)
class UpdateRankingCommandImpl implements RankingSubProcessCommand {

    private final RankingUpdateStrategyFactory rankingUpdateStrategyFactory;
    private final int rankingUpdateLimit;

    @Autowired
    public UpdateRankingCommandImpl(RankingUpdateStrategyFactory rankingUpdateStrategyFactory,
                                    @Value("${ranking-update.data-extraction.limit}") int rankingUpdateLimit) {
        if (log.isTraceEnabled()) {
            log.trace("UpdateRankingCommandImpl.UpdateRankingCommandImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingUpdateStrategyFactory = {}, rankingUpdateLimit = {}", rankingUpdateStrategyFactory, rankingUpdateLimit);
        }

        this.rankingUpdateStrategyFactory = rankingUpdateStrategyFactory;
        this.rankingUpdateLimit = rankingUpdateLimit;
    }

    @Override
    public void execute(long awardPeriodId) {//TODO: manage tied between each chunks
        if (log.isTraceEnabled()) {
            log.trace("UpdateRankingCommandImpl.execute");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }

        int pageNumber = 0;
        int trxCount;
        RankingUpdateStrategy rankingUpdateStrategy = getRankingUpdateStrategy();
        do {
            SimplePageRequest pageRequest = SimplePageRequest.of(pageNumber++, rankingUpdateLimit);
            trxCount = rankingUpdateStrategy.process(awardPeriodId, pageRequest);
        } while (rankingUpdateLimit == trxCount);
    }

    public RankingUpdateStrategy getRankingUpdateStrategy() {
        return rankingUpdateStrategyFactory.create();
    }

}
