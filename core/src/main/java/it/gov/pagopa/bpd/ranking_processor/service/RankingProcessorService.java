package it.gov.pagopa.bpd.ranking_processor.service;

import java.time.LocalTime;

/**
 * Service to orchestrate others services in order to execute the ranking processor process
 */
public interface RankingProcessorService {

    String PROCESS_NAME = "RANKING-PROCESSOR";

    void process(Long awardPeriodId, LocalTime stopTime);

}
