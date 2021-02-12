package it.gov.pagopa.bpd.ranking_processor.service;

/**
 * Service to orchestrate others services in order to execute the ranking processor process
 */
public interface RankingProcessorService {

    void process(Long awardPeriodId);

}
