package it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util;

import java.sql.Statement;
import java.util.function.IntPredicate;

public final class DaoHelper {

    public static IntPredicate isStatementResultKO = value -> value != 1 && value != Statement.SUCCESS_NO_INFO;

    private DaoHelper() {
    }
}
