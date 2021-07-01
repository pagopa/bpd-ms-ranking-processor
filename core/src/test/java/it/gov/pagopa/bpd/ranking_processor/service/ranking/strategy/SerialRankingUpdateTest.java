package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

public class SerialRankingUpdateTest extends RankingUpdateStrategyTemplateTest {

    private final RankingUpdateStrategy rankingUpdateStrategy;

    public SerialRankingUpdateTest() {
        this.rankingUpdateStrategy = new SerialRankingUpdate(citizenRankingDaoMock, true, Integer.MAX_VALUE);
    }

    @Override
    public RankingUpdateStrategy getRankingUpdateService() {
        return rankingUpdateStrategy;
    }

}