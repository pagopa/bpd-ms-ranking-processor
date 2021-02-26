package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.mockito.BDDMockito;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

public class TotalTransferCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    public TotalTransferCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new TotalTransferCashbackUpdate(winningTransactionDaoMock, citizenRankingDaoMock, beanFactoryMock);
    }

    @Override
    protected void verifyTrxToProcess(SimplePageRequest pageRequest, AwardPeriod awardPeriod) {
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findTotalTransferToProcess(eq(awardPeriod.getAwardPeriodId()), eq(toPageable(pageRequest)));
    }

    @Override
    public CashbackUpdateStrategy getCashbackUpdateService() {
        return cashbackUpdateStrategy;
    }

}