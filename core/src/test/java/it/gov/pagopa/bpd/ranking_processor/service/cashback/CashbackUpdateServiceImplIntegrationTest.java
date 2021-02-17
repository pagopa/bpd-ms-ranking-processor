package it.gov.pagopa.bpd.ranking_processor.service.cashback;


import org.junit.runner.RunWith;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

//@JdbcTest
//@AutoConfigureTestDatabase
//@Sql({"schema.sql", "test-data.sql"})
@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = {WinningTransactionDao.class, CashbackProcessingServiceImpl.class})
@TestPropertySource(properties = {
        "ranking-processor-cashback-processing.data-extraction.limit=10",
        "winning-transaction.spring.datasource.driver-class-name=org.h2.Driver",
        "winning-transaction.spring.datasource.url=jdbc:h2:mem:db;DB_CLOSE_DELAY=-1",
        "winning-transaction.spring.datasource.username=sa",
        "winning-transaction.spring.datasource.password=sa",
        "citizen.spring.datasource.xa.dataSourceClassName=org.h2.jdbcx.JdbcDataSource",
        "citizen.spring.datasource.driver-class-name=org.h2.Driver",
        "citizen.spring.datasource.url=jdbc:h2:mem:db;DB_CLOSE_DELAY=-1",
        "citizen.spring.datasource.username=sa",
        "citizen.spring.datasource.password=sa",
        "citizen.spring.datasource.xa.dataSourceClassName=org.h2.jdbcx.JdbcDataSource"
})
public abstract class CashbackUpdateServiceImplIntegrationTest {

}