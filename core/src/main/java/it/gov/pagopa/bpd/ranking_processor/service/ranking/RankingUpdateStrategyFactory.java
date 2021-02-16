package it.gov.pagopa.bpd.ranking_processor.service.ranking;

/**
 * Abstract Factory of {@link RankingUpdateStrategy}
 */
public interface RankingUpdateStrategyFactory {

    RankingUpdateStrategy create();

}
