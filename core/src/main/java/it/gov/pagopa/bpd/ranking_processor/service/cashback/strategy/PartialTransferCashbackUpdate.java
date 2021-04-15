package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Implementation of {@link CashbackUpdateStrategyTemplate} to handle partial transfer
 */
@Slf4j
@Service
@Conditional(CashbackUpdatePartialTransferEnabledCondition.class)
class PartialTransferCashbackUpdate extends CashbackUpdateStrategyTemplate {

    private final int dataExtractionLimit;


    @Autowired
    public PartialTransferCashbackUpdate(WinningTransactionDao winningTransactionDao,
                                         CitizenRankingDao citizenRankingDao,
                                         BeanFactory beanFactory,
                                         @Value("${cashback-update.partial-transfer.data-extraction.limit}") int dataExtractionLimit) {
        super(winningTransactionDao,
                citizenRankingDao,
                beanFactory.getBean(PartialTransferAggregator.class));
        this.dataExtractionLimit = dataExtractionLimit;
    }


    @Override
    public int getDataExtractionLimit() {
        return dataExtractionLimit;
    }

    @Override
    protected List<WinningTransaction> retrieveTransactions(long awardPeriodId, Pageable pageable) {
        return winningTransactionDao.findPartialTranferToProcess(awardPeriodId, pageable);
    }

}
