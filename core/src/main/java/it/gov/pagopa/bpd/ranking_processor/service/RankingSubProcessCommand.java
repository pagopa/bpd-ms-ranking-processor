package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;

import java.time.LocalDateTime;

/**
 * Command Pattern to incapsulate subprocesses logic
 */
public interface RankingSubProcessCommand {

    void execute(AwardPeriod awardPeriod, LocalDateTime stopDateTime);

}
