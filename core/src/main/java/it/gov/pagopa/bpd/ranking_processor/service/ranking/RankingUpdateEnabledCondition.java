package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

class RankingUpdateEnabledCondition extends AllNestedConditions {

    public RankingUpdateEnabledCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "ranking-update", name = "enable", havingValue = "true")
    public static class RankingUpdateEnabled {
    }
}