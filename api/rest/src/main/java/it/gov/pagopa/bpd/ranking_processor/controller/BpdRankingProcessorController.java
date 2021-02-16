package it.gov.pagopa.bpd.ranking_processor.controller;

import io.swagger.annotations.Api;
import it.gov.pagopa.bpd.ranking_processor.controller.model.RankingProcessorDto;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.validation.Valid;

/**
 * Controller to expose MicroService
 */
@Api(tags = "Bonus Pagamenti Digitali ranking-processor Controller")
@RequestMapping("/bpd/ranking-processor")
public interface BpdRankingProcessorController {

    @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseStatus(HttpStatus.OK)
    void execute(@RequestBody @Valid RankingProcessorDto rankingProcessorDto);
}
