package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Concrete Factory of {@link ExecutionStrategyFactory}
 */
@Slf4j
@Service
class ExecutionStrategyFactoryImpl implements ExecutionStrategyFactory {

    private final BeanFactory beanFactory;
    private final boolean parallelEnabled;

    @Autowired
    public ExecutionStrategyFactoryImpl(BeanFactory beanFactory,
                                        @Value("${cashback-update.parallel.enable}") boolean parallelEnabled) {
        this.beanFactory = beanFactory;
        this.parallelEnabled = parallelEnabled;
    }

    @Override
    public ExecutionStrategy create() {
        if (parallelEnabled)
            return beanFactory.getBean(ParallelExecutionStrategy.class);
        else
            return beanFactory.getBean(SerialExecutionStrategy.class);
    }
}
