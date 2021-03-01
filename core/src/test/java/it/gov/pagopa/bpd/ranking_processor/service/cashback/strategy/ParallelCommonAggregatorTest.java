package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

public class ParallelCommonAggregatorTest extends CommonAggregatorTest {

    public ParallelCommonAggregatorTest() {
        super(new ParallelExecutionStrategy());
    }

}