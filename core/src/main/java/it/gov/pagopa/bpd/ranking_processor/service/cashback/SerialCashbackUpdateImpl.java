package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Serial implementation of {@link CashbackUpdateStrategyTemplate}
 */
@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
class SerialCashbackUpdateImpl extends CashbackUpdateStrategyTemplate {

    @Autowired
    public SerialCashbackUpdateImpl(WinningTransactionDao winningTransactionDao,
                                    CitizenRankingDao citizenRankingDao) {
        super(winningTransactionDao, citizenRankingDao);

        if (log.isTraceEnabled()) {
            log.trace("SerialCashbackUpdateImpl.SerialCashbackUpdateImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("winningTransactionDao = {}, citizenRankingDao = {}", winningTransactionDao, citizenRankingDao);
        }
    }


    @Override
    protected Map<String, CitizenRanking> aggregateData(Stream<CitizenRanking> stream,
                                                        Function<CitizenRanking, String> keyMapper,
                                                        Function<CitizenRanking, CitizenRanking> valueMapper,
                                                        BinaryOperator<CitizenRanking> mergeFunction) {
        if (log.isTraceEnabled()) {
            log.trace("SerialCashbackUpdateImpl.aggregateData");
        }
        if (log.isDebugEnabled()) {
            log.debug("stream = {}, keyMapper = {}, valueMapper = {}, mergeFunction = {}", stream, keyMapper, valueMapper, mergeFunction);
        }

        return stream.collect(Collectors.toMap(keyMapper, valueMapper, mergeFunction));
    }

}
