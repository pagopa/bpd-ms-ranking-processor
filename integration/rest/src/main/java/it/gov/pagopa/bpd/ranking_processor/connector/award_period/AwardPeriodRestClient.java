package it.gov.pagopa.bpd.ranking_processor.connector.award_period;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * AwardPeriod Rest Client
 */
@FeignClient(name = "${rest-client.award-period.serviceCode}", url = "${rest-client.award-period.base-url}")
public interface AwardPeriodRestClient {

    @GetMapping(value = "${rest-client.award-period.actives.url}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    List<AwardPeriod> getActiveAwardPeriods();

    @GetMapping(value = "${rest-client.award-period.findById.url}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    AwardPeriod findById(@PathVariable("id") long awardPeriodId);

}
