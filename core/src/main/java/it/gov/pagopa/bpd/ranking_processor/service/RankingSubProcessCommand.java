package it.gov.pagopa.bpd.ranking_processor.service;

/**
 * Command Pattern to incapsulate subprocesses logic
 */
public interface RankingSubProcessCommand {

    void execute(long awardPeriodId);

}
