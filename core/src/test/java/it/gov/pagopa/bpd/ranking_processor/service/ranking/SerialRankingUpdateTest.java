package it.gov.pagopa.bpd.ranking_processor.service.ranking;

public class SerialRankingUpdateTest extends RankingUpdateStrategyTemplateTest {

    private final RankingUpdateStrategy rankingUpdateStrategy;

    public SerialRankingUpdateTest() {
        this.rankingUpdateStrategy = new SerialRankingUpdate(citizenRankingDaoMock);
    }

    @Override
    public RankingUpdateStrategy getRankingUpdateService() {
        return rankingUpdateStrategy;
    }

}