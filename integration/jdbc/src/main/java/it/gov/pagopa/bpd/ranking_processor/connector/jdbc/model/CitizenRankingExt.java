package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.data.relational.core.mapping.Column;

import java.time.OffsetDateTime;

@Data
@Builder
@EqualsAndHashCode(of = {"awardPeriodId"}, callSuper = false)
@ToString
public class CitizenRankingExt {

    @Column("award_period_id_n")
    private Long awardPeriodId;
    @Column("min_transaction_n")
    private Long minTransactionNumber;
    @Column("max_transaction_n")
    private Long maxTransactionNumber;
    @Column("total_participants")
    private Long totalParticipants;
    @Column("ranking_min_n")
    private Long minPosition;
    @Column("period_cashback_max_n")
    private Long maxPeriodCashback;
    @Column("insert_date_t")
    private OffsetDateTime insertDate;
    @Column("insert_user_s")
    private String insertUser;
    @Column("update_date_t")
    private OffsetDateTime updateDate;
    @Column("update_user_s")
    private String updateUser;

}
