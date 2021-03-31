package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

class CashbackUpdatePartialTransferEnabledCondition extends AllNestedConditions {

    public CashbackUpdatePartialTransferEnabledCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "cashback-update.partial-transfer", name = "enable", havingValue = "true")
    public static class CashbackUpdatePartialTransferEnabled {
    }
}