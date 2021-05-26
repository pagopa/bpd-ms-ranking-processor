package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@Service
public class DailyPaymentLimitCleanUpServiceImpl implements DailyPaymentLimitCleanUpService{//TODO test

    private final DailyPaymentLimitUserCleanUpService dailyPaymentLimitUserCleanUpService;
    private final int threadPool;

    @Autowired
    public DailyPaymentLimitCleanUpServiceImpl(DailyPaymentLimitUserCleanUpService dailyPaymentLimitUserCleanUpService, @Value("${check.daily-payment-limit.thread-pool:1}") int threadPool) {
        this.dailyPaymentLimitUserCleanUpService = dailyPaymentLimitUserCleanUpService;
        this.threadPool = threadPool;
    }

    @Override
    public void cleanUpService(Map<String, List<Triple<LocalDate, String, List<WinningTransaction>>>> validPaymentsMap, Long awardPeriodId) {
        if(null != validPaymentsMap && !validPaymentsMap.isEmpty()){
            ExecutorService executor = Executors.newFixedThreadPool(threadPool);
            List<Future<?>> futureList = new LinkedList<>();
            for (Map.Entry<String, List<Triple<LocalDate, String, List<WinningTransaction>>>> validPayments : validPaymentsMap.entrySet()) {
                futureList.add(executor.submit(()-> dailyPaymentLimitUserCleanUpService.cleanUpUserService(validPayments.getKey(), validPayments.getValue(), awardPeriodId)));
            }

            for (Future<?> future : futureList) {
                try{
                    future.get();
                }catch(Exception e){
                    throw new RuntimeException("Something gone wrong with clean-up", e);
                }
            }

            //TODO updateRanking
        }
    }
}
