package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.AllArgsConstructor;
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

    String idTrxAcquirer;
    String acquirerCode;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    OffsetDateTime trxDate;
    String operationType;
    BigDecimal amount;
    BigDecimal score;
    String acquirerId;
    String fiscalCode;

    public WinningTransactionId getId() {
        return new WinningTransactionId(idTrxAcquirer, acquirerCode, trxDate, operationType, acquirerId);
    }

    public enum TransactionType {
        PAYMENT,
        TOTAL_TRANSFER,
        PARTIAL_TRANSFER
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class WinningTransactionId implements Serializable {
        String idTrxAcquirer;
        String acquirerCode;
        OffsetDateTime trxDate;
        String operationType;
        String acquirerId;
    }

}

