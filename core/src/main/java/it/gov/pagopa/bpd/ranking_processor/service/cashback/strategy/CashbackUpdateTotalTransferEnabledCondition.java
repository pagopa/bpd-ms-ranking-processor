package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

class CashbackUpdateTotalTransferEnabledCondition extends AllNestedConditions {

    public CashbackUpdateTotalTransferEnabledCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "cashback-update.total-transfer", name = "enable", havingValue = "true")
    public static class CashbackUpdateTotalTransferEnabled {
    }
}