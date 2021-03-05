package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Concrete Factory of {@link RankingUpdateStrategy}
 */
@Slf4j
@Service
class RankingUpdateStrategyFactoryImpl implements RankingUpdateStrategyFactory {

    private final BeanFactory beanFactory;
    private final boolean parallelEnabled;


    @Autowired
    public RankingUpdateStrategyFactoryImpl(BeanFactory beanFactory,
                                            @Value("${ranking-update.parallel.enable}") boolean parallelEnabled) {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyFactoryImpl.RankingUpdateStrategyFactoryImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("beanFactory = {}, parallelEnabled = {}", beanFactory, parallelEnabled);
        }

        this.beanFactory = beanFactory;
        this.parallelEnabled = parallelEnabled;
    }


    @Override
    public RankingUpdateStrategy create() {
        if (log.isTraceEnabled()) {
            log.trace("RankingUpdateStrategyFactoryImpl.create");
        }

        RankingUpdateStrategy bean;

        if (parallelEnabled) {
            bean = beanFactory.getBean(ParallelRankingUpdate.class);

        } else {
            bean = beanFactory.getBean(SerialRankingUpdate.class);
        }

        return bean;
    }

}
