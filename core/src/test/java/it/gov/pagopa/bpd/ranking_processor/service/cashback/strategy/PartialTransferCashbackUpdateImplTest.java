package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.mockito.BDDMockito;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

public class PartialTransferCashbackUpdateImplTest extends CashbackUpdateStrategyTemplateTest {

    private final CashbackUpdateStrategy cashbackUpdateStrategy;

    public PartialTransferCashbackUpdateImplTest() {
        this.cashbackUpdateStrategy = new PartialTransferCashbackUpdate(winningTransactionDaoMock, citizenRankingDaoMock, beanFactoryMock);
    }

    @Override
    protected void verifyTrxToProcess(SimplePageRequest pageRequest, AwardPeriod awardPeriod) {
        BDDMockito.verify(winningTransactionDaoMock, times(1))
                .findPartialTranferToProcess(eq(awardPeriod.getAwardPeriodId()), eq(toPageable(pageRequest)));
    }

    @Override
    public CashbackUpdateStrategy getCashbackUpdateService() {
        return cashbackUpdateStrategy;
    }

}