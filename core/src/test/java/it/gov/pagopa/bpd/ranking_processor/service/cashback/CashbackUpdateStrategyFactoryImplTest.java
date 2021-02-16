package it.gov.pagopa.bpd.ranking_processor.service.cashback;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanFactory;

public class CashbackUpdateStrategyFactoryImplTest {

    private final BeanFactory beanFactoryMock;
    private final CashbackUpdateStrategyFactoryImpl serialCashbackUpdateFactory;
    private final CashbackUpdateStrategyFactoryImpl parallelCashbackUpdateFactory;


    public CashbackUpdateStrategyFactoryImplTest() {
        beanFactoryMock = Mockito.mock(BeanFactory.class);
        serialCashbackUpdateFactory = new CashbackUpdateStrategyFactoryImpl(beanFactoryMock, false);
        parallelCashbackUpdateFactory = new CashbackUpdateStrategyFactoryImpl(beanFactoryMock, true);

        initMocks();
    }


    private void initMocks() {
        BDDMockito.doAnswer(invocationOnMock -> {
            Class argument = invocationOnMock.getArgument(0, Class.class);
            if (ParallelCashbackUpdateImpl.class.getName().equals(argument.getName()))
                return new ParallelCashbackUpdateImpl(null, null);
            else if (SerialCashbackUpdateImpl.class.getName().equals(argument.getName()))
                return new SerialCashbackUpdateImpl(null, null);
            else
                throw new IllegalArgumentException();
        })
                .when(beanFactoryMock)
                .getBean(Mockito.any(Class.class));
    }


    @Test
    public void create_OK_serial() {
        CashbackUpdateStrategy serialCashbackUpdateStrategy = serialCashbackUpdateFactory.create();
        Assert.assertNotNull(serialCashbackUpdateStrategy);
        Assert.assertTrue(SerialCashbackUpdateImpl.class.isAssignableFrom(serialCashbackUpdateStrategy.getClass()));
    }

    @Test
    public void create_OK_parallel() {
        CashbackUpdateStrategy parallelCashbackUpdateStrategy = parallelCashbackUpdateFactory.create();
        Assert.assertNotNull(parallelCashbackUpdateStrategy);
        Assert.assertTrue(ParallelCashbackUpdateImpl.class.isAssignableFrom(parallelCashbackUpdateStrategy.getClass()));
    }

}