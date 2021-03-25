package it.gov.pagopa.bpd.ranking_processor.controller.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * Data Transfer Object (input) for {@link it.gov.pagopa.bpd.ranking_processor.controller.BpdRankingProcessorController}
 */
@Data
@ToString
public class RankingProcessorDto {

    @NotNull
    @JsonProperty(required = true)
    private Long awardPeriodId;

    private LocalDateTime stopDateTime;

}
