package it.gov.pagopa.bpd.ranking_processor.service.ranking;

public class ParallelRankingUpdateImplTest extends RankingUpdateStrategyTemplateTest {

    private final RankingUpdateStrategy rankingUpdateStrategy;

    public ParallelRankingUpdateImplTest() {
        this.rankingUpdateStrategy = new ParallelRankingUpdateImpl(citizenRankingDaoMock);
    }

    @Override
    public RankingUpdateStrategy getRankingUpdateService() {
        return rankingUpdateStrategy;
    }

}