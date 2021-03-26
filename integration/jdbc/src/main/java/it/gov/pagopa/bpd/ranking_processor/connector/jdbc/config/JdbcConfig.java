package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.data.transaction.ChainedTransactionManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

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
        return new JdbcTemplate(citizenDataSource());
    }

    @Bean("citizenDataSource")
    @Primary
    @ConfigurationProperties(prefix = "citizen.spring.datasource.hikari")
    public DataSource citizenDataSource() {
        return citizenDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean("citizenDataSourceProperties")
    @Primary
    @ConfigurationProperties("citizen.spring.datasource")
    public DataSourceProperties citizenDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean("winningTransactionJdbcTemplate")
    public JdbcTemplate winningTransactionJdbcTemplate() {
        return new JdbcTemplate(winningTransactionDataSource());
    }

    @Bean("winningTransactionDataSource")
    @ConfigurationProperties(prefix = "winning-transaction.spring.datasource.hikari")
    public DataSource winningTransactionDataSource() {
        return winningTransactionDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean("winningTransactionDataSourceProperties")
    @ConfigurationProperties("winning-transaction.spring.datasource")
    public DataSourceProperties winningTransactionDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean("winningTransactionTransactionManager")
    public PlatformTransactionManager winningTransactionTransactionManager(@Qualifier("winningTransactionDataSource")
                                                                                   DataSource winningTransactionDataSource) {
        return new DataSourceTransactionManager(winningTransactionDataSource);
    }

    @Bean("citizenTransactionManager")
    public PlatformTransactionManager citizenTransactionManager(@Qualifier("citizenDataSource")
                                                                        DataSource citizenDataSource) {
        return new DataSourceTransactionManager(citizenDataSource);
    }


    @Bean("chainedTransactionManager")
    public ChainedTransactionManager transactionManager(@Qualifier("winningTransactionTransactionManager")
                                                                PlatformTransactionManager winningTransactionTransactionManager,
                                                        @Qualifier("citizenTransactionManager")
                                                                PlatformTransactionManager citizenTransactionManager) {
        return new ChainedTransactionManager(winningTransactionTransactionManager, citizenTransactionManager);
    }

//    @Bean
//    public InitializingBean init(@Qualifier("winningTransactionDataSource") DataSource winningTransactionDataSource,
//                                 @Qualifier("citizenDataSource") DataSource citizenDataSource) {
//        return () -> {
//            winningTransactionDataSource.getConnection();
//            citizenDataSource.getConnection();
//        };
//    }


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
