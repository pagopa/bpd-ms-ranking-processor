package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.data.relational.core.mapping.Column;

import java.math.BigDecimal;

@Data
@Builder
@EqualsAndHashCode(of = {"fiscalCode", "awardPeriodId"}, callSuper = false)
@ToString(of = {"fiscalCode", "ranking", "transactionNumber", "totalCashback", "awardPeriodId"})
public class CitizenRanking {

    @Column("fiscal_code_c")
    private String fiscalCode;
    @Column("award_period_id_n")
    private Long awardPeriodId;
    @Column("cashback_n")
    private BigDecimal totalCashback;
    @Column("transaction_n")
    private Long transactionNumber;
    @Column("ranking_n")
    private Long ranking;
//    @Column("ranking_min_n")
//    private Long rankingMinRequired;
//    @Column("")
//    private BigDecimal maxTotalCashback;

}
