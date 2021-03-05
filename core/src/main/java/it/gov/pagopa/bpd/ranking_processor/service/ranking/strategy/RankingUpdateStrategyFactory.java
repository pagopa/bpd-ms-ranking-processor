package it.gov.pagopa.bpd.ranking_processor.service.ranking.strategy;

/**
 * Abstract Factory of {@link RankingUpdateStrategy}
 */
public interface RankingUpdateStrategyFactory {

    RankingUpdateStrategy create();

}
