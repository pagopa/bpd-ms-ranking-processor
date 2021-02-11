package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@Builder
@EqualsAndHashCode(of = {"fiscalCode", "awardPeriodId"}, callSuper = false)
public class CitizenRanking implements Comparable<CitizenRanking> {

    private String fiscalCode;
    private Long awardPeriodId;
    private BigDecimal totalCashback;
    private Long transactionNumber;
    private Long ranking;
    private Long rankingMinRequired;
    private BigDecimal maxTotalCashback;

    @Override
    public int compareTo(CitizenRanking cr) {
        return ranking.compareTo(cr.getRanking());
    }

}
