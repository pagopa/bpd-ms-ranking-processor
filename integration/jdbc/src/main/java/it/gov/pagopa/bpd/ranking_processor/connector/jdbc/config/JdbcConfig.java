package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@PropertySources({
        @PropertySource("classpath:config/citizenJdbcConfig.properties"),
        @PropertySource("classpath:config/winningTransactionJdbcConfig.properties")
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

    @Bean("winningTransactionJdbcTemplate")
    public JdbcTemplate winningTransactionJdbcTemplate() {
        return new JdbcTemplate(winningTransactionDataSource());
    }

    @Bean(name = "winningTransactionDataSource")
    @ConfigurationProperties(prefix = "winning-transaction.spring.datasource.hikari")
    public DataSource winningTransactionDataSource() {
        return winningTransactionDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "winningTransactionDataSourceProperties")
    @ConfigurationProperties("winning-transaction.spring.datasource")
    public DataSourceProperties winningTransactionDataSourceProperties() {
        return new DataSourceProperties();
    }


//    @Bean
//    public UserTransaction userTransaction() throws Throwable {
//        UserTransactionImp userTransactionImp = new UserTransactionImp();
//        userTransactionImp.setTransactionTimeout(1000);
//        return userTransactionImp;
//    }
//
//    @Bean(initMethod = "init", destroyMethod = "close")
//    public TransactionManager transactionManager() throws Throwable {
//        UserTransactionManager userTransactionManager = new UserTransactionManager();
//        userTransactionManager.setForceShutdown(false);
//        return userTransactionManager;
//    }
//
//    @Bean
//    public PlatformTransactionManager platformTransactionManager() throws Throwable {
//        return new JtaTransactionManager(userTransaction(), transactionManager());
//    }

}
