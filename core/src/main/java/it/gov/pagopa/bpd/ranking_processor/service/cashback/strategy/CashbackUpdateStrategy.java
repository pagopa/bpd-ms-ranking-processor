package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;

import java.util.function.BinaryOperator;

/**
 * A Strategy Pattern to manage the Business Logic related to cashback update process
 */
public interface CashbackUpdateStrategy {

    BinaryOperator<CitizenRanking> CASHBACK_MAPPER = (cr1, cr2) -> {
        cr1.setTotalCashback(cr1.getTotalCashback().add(cr2.getTotalCashback()));
        cr1.setTransactionNumber(cr1.getTransactionNumber() + cr2.getTransactionNumber());
        return cr1;
    };

    int process(final AwardPeriod awardPeriod, WinningTransaction.TransactionType transactionType, SimplePageRequest simplePageRequest);

}
