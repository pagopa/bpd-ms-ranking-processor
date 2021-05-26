package it.gov.pagopa.bpd.ranking_processor.service;

public interface DailyPaymentLimitDetectorService {
    void checkPaymentLimit(Long awardPeriodId);
}
