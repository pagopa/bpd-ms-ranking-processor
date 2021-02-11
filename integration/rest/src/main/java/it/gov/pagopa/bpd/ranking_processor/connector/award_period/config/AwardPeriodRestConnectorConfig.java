package it.gov.pagopa.bpd.ranking_processor.connector.award_period.config;

import it.gov.pagopa.bpd.common.connector.config.RestConnectorConfig;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Import(RestConnectorConfig.class)
@EnableFeignClients(clients = AwardPeriodRestClient.class)
@PropertySource("classpath:config/award_period/rest-client.properties")
public class AwardPeriodRestConnectorConfig {
}
