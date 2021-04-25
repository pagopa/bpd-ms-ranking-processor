package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.*;
import org.springframework.data.relational.core.mapping.Column;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@Builder
@EqualsAndHashCode(of = {"fiscalCode", "awardPeriodId"}, callSuper = false)
@ToString(of = {"fiscalCode", "ranking", "transactionNumber", "totalCashback", "awardPeriodId"})
public class CitizenRanking {

    @Column("fiscal_code_c")
    private String fiscalCode;
    @Column("award_period_id_n")
    private Long awardPeriodId;
    @Column("transaction_n")
    private Long transactionNumber;
    @Column("cashback_n")
    private BigDecimal totalCashback;
    @Column("ranking_n")
    private Long ranking;
    @Column("insert_date_t")
    private OffsetDateTime insertDate;
    @Column("insert_user_s")
    private String insertUser;
    @Column("update_date_t")
    private OffsetDateTime updateDate;
    @Column("update_user_s")
    private String updateUser;


    @Data
    @AllArgsConstructor
    public static class FilterCriteria {
        private Long awardPeriodId;
        private OffsetDateTime updateDate;
    }

}
