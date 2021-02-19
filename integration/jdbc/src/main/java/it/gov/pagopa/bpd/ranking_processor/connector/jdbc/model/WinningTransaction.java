package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@Builder
@EqualsAndHashCode(of = {"idTrxAcquirer", "acquirerCode", "trxDate", "operationType", "acquirerId"}, callSuper = false)
public class WinningTransaction implements Serializable {

    private String idTrxAcquirer;
    private String acquirerCode;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime trxDate;
    private String operationType;
    private BigDecimal amount;
    private BigDecimal score;
    private String acquirerId;
    private String fiscalCode;


    public enum TransactionType {
        PAYMENT,
        TOTAL_TRANSFER
//        PARTIAL_TRANSFER
    }

}

