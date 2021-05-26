package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.OffsetDateTime;

@Data
@Builder
@EqualsAndHashCode(of = {"fiscalCode", "merchantId", "trxDate", "insertDate"}, callSuper = false)
public class ActiveUserWinningTransaction implements Serializable {

    private String fiscalCode;
    private String merchantId;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    private LocalDate trxDate;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime insertDate;
}
