package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Implementation of {@link CashbackUpdateStrategyTemplate} to handle total transfer
 */
@Slf4j
@Service
class TotalTransferCashbackUpdate extends CashbackUpdateStrategyTemplate {

    @Autowired
    public TotalTransferCashbackUpdate(WinningTransactionDao winningTransactionDao,
                                       CitizenRankingDao citizenRankingDao,
                                       BeanFactory beanFactory) {
        super(winningTransactionDao, citizenRankingDao, beanFactory.getBean(StandardAggregator.class));
    }


    @Override
    protected List<WinningTransaction> retrieveTransactions(long awardPeriodId, Pageable pageRequest) {
        return winningTransactionDao.findTotalTransferToProcess(awardPeriodId, pageRequest);
    }

}
