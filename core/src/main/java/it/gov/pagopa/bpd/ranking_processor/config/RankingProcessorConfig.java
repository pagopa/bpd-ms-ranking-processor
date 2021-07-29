package it.gov.pagopa.bpd.ranking_processor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:config/cashbackProcessingConfig.properties")
class RankingProcessorConfig {
}
