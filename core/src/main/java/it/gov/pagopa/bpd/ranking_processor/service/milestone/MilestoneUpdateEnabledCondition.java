package it.gov.pagopa.bpd.ranking_processor.service.milestone;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

class MilestoneUpdateEnabledCondition extends AllNestedConditions {

    public MilestoneUpdateEnabledCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "milestone-update", name = "enable", havingValue = "true")
    public static class MilestoneUpdateEnabled {
    }
}