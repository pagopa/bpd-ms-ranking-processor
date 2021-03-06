package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

public class ParallelRankingUpdateTest extends RankingUpdateStrategyTemplateTest {

    private final RankingUpdateStrategy rankingUpdateStrategy;

    public ParallelRankingUpdateTest() {
        this.rankingUpdateStrategy = new ParallelRankingUpdate(citizenRankingDaoMock, true, Integer.MAX_VALUE);
    }

    @Override
    public RankingUpdateStrategy getRankingUpdateService() {
        return rankingUpdateStrategy;
    }

}