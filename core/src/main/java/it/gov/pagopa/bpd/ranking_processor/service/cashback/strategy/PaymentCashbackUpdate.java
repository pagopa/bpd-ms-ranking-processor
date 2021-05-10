package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao.FIND_TRX_TO_PROCESS_PAGEABLE_SORT;

/**
 * Implementation of {@link CashbackUpdateStrategyTemplate} to handle payment
 */
@Slf4j
@Service
@Conditional(CashbackUpdatePaymentEnabledCondition.class)
class PaymentCashbackUpdate extends CashbackUpdateStrategyTemplate {

    private final int dataExtractionLimit;


    @Autowired
    public PaymentCashbackUpdate(WinningTransactionDao winningTransactionDao,
                                 CitizenRankingDao citizenRankingDao,
                                 BeanFactory beanFactory,
                                 @Value("${cashback-update.payment.data-extraction.limit}") int dataExtractionLimit) {
        super(winningTransactionDao,
                citizenRankingDao,
                beanFactory.getBean(CommonAggregator.class));
        this.dataExtractionLimit = dataExtractionLimit;
    }


    @Override
    public int getDataExtractionLimit() {
        return dataExtractionLimit;
    }

    @Override
    protected List<WinningTransaction> retrieveTransactions(long awardPeriodId, Pageable pageable) {
        Pageable pageRequest = PageRequest.of(pageable.getPageNumber(),
                pageable.getPageSize(),
                FIND_TRX_TO_PROCESS_PAGEABLE_SORT);
        return winningTransactionDao.findPaymentToProcess(awardPeriodId, pageRequest);
    }

}
