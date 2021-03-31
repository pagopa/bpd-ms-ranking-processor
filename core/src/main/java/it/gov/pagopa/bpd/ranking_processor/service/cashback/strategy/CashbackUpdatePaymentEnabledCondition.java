package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ConfigurationCondition;

class CashbackUpdatePaymentEnabledCondition extends AllNestedConditions {

    public CashbackUpdatePaymentEnabledCondition() {
        super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "cashback-update.payment", name = "enable", havingValue = "true")
    public static class CashbackUpdatePaymentEnabled {
    }
}