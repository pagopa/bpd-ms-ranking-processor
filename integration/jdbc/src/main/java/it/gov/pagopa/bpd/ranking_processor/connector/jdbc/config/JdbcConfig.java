package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@PropertySources({
        @PropertySource("classpath:config/citizenJdbcConfig.properties"),
        @PropertySource("classpath:config/transactionJdbcConfig.properties")
})
class JdbcConfig {

    @Bean("citizenJdbcTemplate")
    @Primary
    public JdbcTemplate citizenJdbcTemplate() {
        return new JdbcTemplate(rtdDataSource());
    }

    @Bean(name = "citizenDataSource")
    @Primary
    @ConfigurationProperties(prefix = "citizen.spring.datasource.hikari")
    public DataSource rtdDataSource() {
        return citizenDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "citizenDataSourceProperties")
    @Primary
    @ConfigurationProperties("citizen.spring.datasource")
    public DataSourceProperties citizenDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean("transactionJdbcTemplate")
    public JdbcTemplate transactionJdbcTemplate() {
        return new JdbcTemplate(transactionDataSource());
    }

    @Bean(name = "transactionDataSource")
    @ConfigurationProperties(prefix = "transaction.spring.datasource.hikari")
    public DataSource transactionDataSource() {
        return transactionDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "transactionDataSourceProperties")
    @ConfigurationProperties("transaction.spring.datasource")
    public DataSourceProperties transactionDataSourceProperties() {
        return new DataSourceProperties();
    }

}
