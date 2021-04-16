package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;

/**
 * A Strategy Pattern to manage the Business Logic related to ranking update process
 */
public interface RankingUpdateStrategy {

    int process(AwardPeriod awardPeriod, SimplePageRequest simplePageRequest);

    void updateRankingExt(AwardPeriod awardPeriod);

}
