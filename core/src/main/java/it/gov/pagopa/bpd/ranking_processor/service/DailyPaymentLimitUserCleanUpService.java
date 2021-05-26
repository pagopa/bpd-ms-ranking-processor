package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import org.apache.commons.lang3.tuple.Triple;

import java.time.LocalDate;
import java.util.List;

public interface DailyPaymentLimitUserCleanUpService {
    void cleanUpUserService(String fiscalCode, List<Triple<LocalDate, String, List<WinningTransaction>>> validPaymentsList, Long awardPeriodId);
}
