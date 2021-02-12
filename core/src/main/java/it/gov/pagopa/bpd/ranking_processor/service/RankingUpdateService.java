package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * A service to manage the Business Logic related to ranking update process
 */
public interface RankingUpdateService {

    int process(long awardPeriodId, MutableInt lastAssignedRanking, SimplePageRequest simplePageRequest);

}
