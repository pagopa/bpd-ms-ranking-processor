package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.List;

@Slf4j
@Service
public class RankingProcessorServiceImpl implements RankingProcessorService {

    private final AwardPeriodRestClient awardPeriodRestClient;
    private final List<RankingSubProcessCommand> subProcesses;
    private final LocalTime stopTime;


    @Autowired
    public RankingProcessorServiceImpl(AwardPeriodRestClient awardPeriodRestClient,
                                       List<RankingSubProcessCommand> subProcesses,
                                       @Value("${ranking-processor.stop-time}")
                                       @DateTimeFormat(iso = DateTimeFormat.ISO.TIME)
                                               LocalTime stopTime) {
        this.awardPeriodRestClient = awardPeriodRestClient;
        this.subProcesses = subProcesses;
        this.stopTime = stopTime;
    }


    @Scheduled(cron = "${ranking-processor.cron}")
    public void execute() {
        log.info("RankingProcessorServiceImpl.execute start");

        List<AwardPeriod> activeAwardPeriods = awardPeriodRestClient.getActiveAwardPeriods();
        activeAwardPeriods.forEach(awardPeriod -> process(awardPeriod, stopTime));

        log.info("RankingProcessorServiceImpl.execute end");
    }

    private void process(AwardPeriod awardPeriod, LocalTime stopTime) {
        if (log.isTraceEnabled()) {
            log.trace("RankingProcessorServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriod);
        }

        if (awardPeriod == null) {
            throw new IllegalArgumentException("awardPeriod can not be null");
        }

        subProcesses.forEach(command -> command.execute(awardPeriod, stopTime));
    }

    @Override
    public void process(Long awardPeriodId, LocalTime stopTime) {
        if (log.isTraceEnabled()) {
            log.trace("RankingProcessorServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, stopDateTime = {}", awardPeriodId, stopTime);
        }

        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }

        AwardPeriod awardPeriod = awardPeriodRestClient.findById(awardPeriodId);
        process(awardPeriod, stopTime);
    }

}

