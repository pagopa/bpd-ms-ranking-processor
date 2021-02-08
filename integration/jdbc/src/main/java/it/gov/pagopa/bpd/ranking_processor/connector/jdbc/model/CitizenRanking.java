package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@Builder
@EqualsAndHashCode(of = {"fiscalCode", "awardPeriodId"}, callSuper = false)
public class CitizenRanking {

    //    @Column(name = "fiscal_code_c")
    private String fiscalCode;
    //    @Column(name = "award_period_id_n")
    private Long awardPeriodId;
    //    @Column(name = "cashback_n")
    private BigDecimal totalCashback;
    //    @Column(name = "transaction_n")
    private Long transactionNumber;
    //    @Column(name = "ranking_n")
    private Long ranking;
    //    @Column(name = "ranking_min_n")
    private Long rankingMinRequired;
    //    @Column(name = "max_cashback_n")
    private BigDecimal maxTotalCashback;

}
