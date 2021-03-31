package it.gov.pagopa.bpd.ranking_processor.service.redis;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

class RedisUpdateEnabledCondition extends AllNestedConditions {

    public RedisUpdateEnabledCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "redis-update", name = "enable", havingValue = "true")
    public static class RedisUpdateEnabled {
    }
}