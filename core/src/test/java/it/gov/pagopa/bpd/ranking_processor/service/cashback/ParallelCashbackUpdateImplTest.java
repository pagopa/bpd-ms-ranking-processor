package it.gov.pagopa.bpd.ranking_processor.service.cashback;

public class ParallelCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    public ParallelCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new ParallelCashbackUpdateImpl(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Override
    public CashbackUpdateStrategy getCashbackUpdateService() {
        return cashbackUpdateStrategy;
    }

}