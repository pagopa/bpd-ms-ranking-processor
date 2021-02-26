package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;

import java.util.Collection;
import java.util.List;

/**
 * A Strategy Pattern to manage the aggregation logic related to cashback update process
 */
public interface AggregatorStrategy {

    Collection<CitizenRanking> aggregate(final AwardPeriod awardPeriod, final List<WinningTransaction> transactions);

}
