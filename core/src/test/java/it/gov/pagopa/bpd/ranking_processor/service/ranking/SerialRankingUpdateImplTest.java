package it.gov.pagopa.bpd.ranking_processor.service.ranking;

public class SerialRankingUpdateImplTest extends RankingUpdateStrategyTemplateTest {

    private final RankingUpdateStrategy rankingUpdateStrategy;

    public SerialRankingUpdateImplTest() {
        this.rankingUpdateStrategy = new SerialRankingUpdateImpl(citizenRankingDaoMock);
    }

    @Override
    public RankingUpdateStrategy getRankingUpdateService() {
        return rankingUpdateStrategy;
    }

}