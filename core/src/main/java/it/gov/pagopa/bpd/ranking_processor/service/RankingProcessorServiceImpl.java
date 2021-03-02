package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class RankingProcessorServiceImpl implements RankingProcessorService {

    private final AwardPeriodRestClient awardPeriodRestClient;
    private final List<RankingSubProcessCommand> subProcesses;


    @Autowired
    public RankingProcessorServiceImpl(AwardPeriodRestClient awardPeriodRestClient,
                                       List<RankingSubProcessCommand> subProcesses) {
        this.awardPeriodRestClient = awardPeriodRestClient;
        this.subProcesses = subProcesses;
    }


    @Scheduled(cron = "${ranking-processor.cron}")
    public void execute() {
        log.info("RankingProcessorServiceImpl.execute start");

        List<AwardPeriod> activeAwardPeriods = awardPeriodRestClient.getActiveAwardPeriods();
        activeAwardPeriods.forEach(this::process);

        log.info("RankingProcessorServiceImpl.execute end");
    }


    @Override
    public void process(Long awardPeriodId) {
        if (log.isTraceEnabled()) {
            log.trace("RankingProcessorServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }

        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }

        //FIXME
        AwardPeriod awardPeriod = awardPeriodRestClient.findById(awardPeriodId);
//        AwardPeriod awardPeriod = AwardPeriod.builder()
//                .awardPeriodId(awardPeriodId)
//                .minPosition(1000L)
//                .maxPeriodCashback(150L)
//                .maxTransactionEvaluated(150L)
//                .cashbackPercentage(10)
//                .build();
        process(awardPeriod);
    }


    private void process(AwardPeriod awardPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("RankingProcessorServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        if (awardPeriod == null) {
            throw new IllegalArgumentException("awardPeriod can not be null");
        }

        subProcesses.forEach(command -> command.execute(awardPeriod));
    }

}

