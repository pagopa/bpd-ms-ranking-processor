package it.gov.pagopa.bpd.ranking_processor.service;

import org.springframework.data.domain.Pageable;

import javax.validation.constraints.NotNull;

/**
 * Service to orchestrate others services in order to execute the Ranking Processor process
 */
public interface RankingProcessorService {

    void process(@NotNull Long awardPeriodId, @NotNull Pageable pageable);

}
