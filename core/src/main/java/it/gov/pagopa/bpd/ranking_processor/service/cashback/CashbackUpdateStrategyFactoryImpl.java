package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Concrete Factory of {@link CashbackUpdateStrategy}
 */
@Slf4j
@Service
class CashbackUpdateStrategyFactoryImpl implements CashbackUpdateStrategyFactory {

    private final BeanFactory beanFactory;
    private final boolean parallelEnabled;


    @Autowired
    public CashbackUpdateStrategyFactoryImpl(BeanFactory beanFactory,
                                             @Value("${cashback-update.parallel.enable}") boolean parallelEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyFactoryImpl.CashbackUpdateStrategyFactoryImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("beanFactory = {}, parallelEnabled = {}", beanFactory, parallelEnabled);
        }

        this.beanFactory = beanFactory;
        this.parallelEnabled = parallelEnabled;
    }


    @Override
    public CashbackUpdateStrategy create() {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyFactoryImpl.create");
        }

        CashbackUpdateStrategy bean;

        if (parallelEnabled) {
            bean = beanFactory.getBean(ParallelCashbackUpdateImpl.class);

        } else {
            bean = beanFactory.getBean(SerialCashbackUpdateImpl.class);
        }

        return bean;
    }

}
