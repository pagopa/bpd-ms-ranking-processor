package it.gov.pagopa.bpd.ranking_processor.service.cashback;

public class SerialCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    public SerialCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new SerialCashbackUpdateImpl(winningTransactionDaoMock, citizenRankingDaoMock);
    }

    @Override
    public CashbackUpdateStrategy getCashbackUpdateService() {
        return cashbackUpdateStrategy;
    }

}