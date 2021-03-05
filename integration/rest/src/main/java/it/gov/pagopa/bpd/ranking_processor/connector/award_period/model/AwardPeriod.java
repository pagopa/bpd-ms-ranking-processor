package it.gov.pagopa.bpd.ranking_processor.connector.award_period.model;

import lombok.*;

import java.time.LocalDate;

/**
 * Resource model for the data recovered through {@link it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient}
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = "awardPeriodId", callSuper = false)
@ToString
public class AwardPeriod {

    private Long awardPeriodId;
    private LocalDate startDate;
    private LocalDate endDate;
    private Integer gracePeriod;
    private Integer maxTransactionCashback;
    private String status;
    private Long minPosition;
    private Long maxPeriodCashback;
    private Long maxTransactionEvaluated;
    private Integer cashbackPercentage;

}
