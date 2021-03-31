package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Concrete Factory of {@link CashbackUpdateStrategy}
 */
@Slf4j
@Service
class CashbackUpdateStrategyFactoryImpl implements CashbackUpdateStrategyFactory {

    private final BeanFactory beanFactory;


    @Autowired
    public CashbackUpdateStrategyFactoryImpl(BeanFactory beanFactory) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyFactoryImpl.CashbackUpdateStrategyFactoryImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("beanFactory = {}", beanFactory);
        }

        this.beanFactory = beanFactory;
    }


    @Override
    public CashbackUpdateStrategy create(WinningTransaction.TransactionType trxType) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyFactoryImpl.create");
        }
        switch (trxType) {
            case PAYMENT:
                return beanFactory.getBeanProvider(PaymentCashbackUpdate.class).getIfAvailable();
            case TOTAL_TRANSFER:
                return beanFactory.getBeanProvider(TotalTransferCashbackUpdate.class).getIfAvailable();
            case PARTIAL_TRANSFER:
                return beanFactory.getBeanProvider(PartialTransferCashbackUpdate.class).getIfAvailable();
            default:
                throw new IllegalArgumentException("TransactionType '" + trxType + "' not handled");
        }
    }

}
