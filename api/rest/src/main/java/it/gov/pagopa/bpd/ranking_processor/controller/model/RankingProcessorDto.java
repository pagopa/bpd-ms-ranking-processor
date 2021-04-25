package it.gov.pagopa.bpd.ranking_processor.controller.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.time.LocalTime;

/**
 * Data Transfer Object (input) for {@link it.gov.pagopa.bpd.ranking_processor.controller.BpdRankingProcessorController}
 */
@Data
@ToString
public class RankingProcessorDto {

    @NotNull
    @JsonProperty(required = true)
    private Long awardPeriodId;

    @ApiModelProperty(example = "00:00:00.000")
    private LocalTime stopTime;

}
