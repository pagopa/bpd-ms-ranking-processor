package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;

import java.time.LocalTime;
import java.util.function.Predicate;

/**
 * Command Pattern to incapsulate subprocesses logic
 */
public interface RankingSubProcessCommand {

    Predicate<LocalTime> isToStop = localTime -> localTime != null && LocalTime.now().isAfter(localTime);

    void execute(AwardPeriod awardPeriod, LocalTime stopTime);

}
