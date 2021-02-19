package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;

/**
 * Command Pattern to incapsulate subprocesses logic
 */
public interface RankingSubProcessCommand {

    void execute(AwardPeriod awardPeriod);

}
