package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;

import java.time.LocalDateTime;
import java.util.function.Predicate;

/**
 * Command Pattern to incapsulate subprocesses logic
 */
public interface RankingSubProcessCommand {

    Predicate<LocalDateTime> isToStop = localDateTime -> localDateTime != null && LocalDateTime.now().isAfter(localDateTime);

    void execute(AwardPeriod awardPeriod, LocalDateTime stopDateTime);

}
