package it.gov.pagopa.bpd.ranking_processor.service.ranking;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanFactory;

public class RankingUpdateStrategyFactoryImplTest {

    private final BeanFactory beanFactoryMock;
    private final RankingUpdateStrategyFactoryImpl serialRankingUpdateFactory;
    private final RankingUpdateStrategyFactoryImpl parallelRankingUpdateFactory;


    public RankingUpdateStrategyFactoryImplTest() {
        beanFactoryMock = Mockito.mock(BeanFactory.class);
        serialRankingUpdateFactory = new RankingUpdateStrategyFactoryImpl(beanFactoryMock, false);
        parallelRankingUpdateFactory = new RankingUpdateStrategyFactoryImpl(beanFactoryMock, true);

        initMocks();
    }


    private void initMocks() {
        BDDMockito.doAnswer(invocationOnMock -> {
            Class argument = invocationOnMock.getArgument(0, Class.class);
            if (ParallelRankingUpdate.class.getName().equals(argument.getName()))
                return new ParallelRankingUpdate(null);
            else if (SerialRankingUpdate.class.getName().equals(argument.getName()))
                return new SerialRankingUpdate(null);
            else
                throw new IllegalArgumentException();
        })
                .when(beanFactoryMock)
                .getBean(Mockito.any(Class.class));
    }


    @Test
    public void create_OK_serial() {
        RankingUpdateStrategy serialRankingUpdateStrategy = serialRankingUpdateFactory.create();
        Assert.assertNotNull(serialRankingUpdateStrategy);
        Assert.assertTrue(SerialRankingUpdate.class.isAssignableFrom(serialRankingUpdateStrategy.getClass()));
    }

    @Test
    public void create_OK_parallel() {
        RankingUpdateStrategy parallelRankingUpdateStrategy = parallelRankingUpdateFactory.create();
        Assert.assertNotNull(parallelRankingUpdateStrategy);
        Assert.assertTrue(ParallelRankingUpdate.class.isAssignableFrom(parallelRankingUpdateStrategy.getClass()));
    }

}