package it.gov.pagopa.bpd.ranking_processor.service;

import javax.validation.constraints.NotNull;

/**
 * Service to orchestrate others services in order to execute the Ranking Update process
 */
public interface RankingUpdateService {

    int process(@NotNull Long awardPeriodId, int pageNumber);

}
