package it.gov.pagopa.bpd.ranking_processor.connector.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
class TransactionDaoImpl implements TransactionDao {

    private final JdbcTemplate citizenJdbcTemplate;
    private final JdbcTemplate transactionJdbcTemplate;

    @Autowired
    public TransactionDaoImpl(
            @Qualifier("citizenJdbcTemplate") JdbcTemplate citizenJdbcTemplate,
            @Qualifier("transactionJdbcTemplate") JdbcTemplate transactionJdbcTemplate) {
        this.citizenJdbcTemplate = citizenJdbcTemplate;
        this.transactionJdbcTemplate = transactionJdbcTemplate;
    }


}
