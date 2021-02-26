package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanFactory;

public class CashbackUpdateStrategyFactoryImplTest {

    private final BeanFactory beanFactoryMock;
    private final CashbackUpdateStrategyFactoryImpl cashbackUpdateFactory;
    private final PaymentCashbackUpdate paymentCashbackUpdateMock;
    private final TotalTransferCashbackUpdate totalTransferCashbackUpdateMock;
    private final PartialTransferCashbackUpdate partialTransferCashbackUpdateMock;


    public CashbackUpdateStrategyFactoryImplTest() {
        beanFactoryMock = Mockito.mock(BeanFactory.class);
        paymentCashbackUpdateMock = Mockito.mock(PaymentCashbackUpdate.class);
        totalTransferCashbackUpdateMock = Mockito.mock(TotalTransferCashbackUpdate.class);
        partialTransferCashbackUpdateMock = Mockito.mock(PartialTransferCashbackUpdate.class);
        cashbackUpdateFactory = new CashbackUpdateStrategyFactoryImpl(beanFactoryMock);

        initMocks();
    }


    private void initMocks() {
        BDDMockito.doAnswer(invocationOnMock -> {
            Class argument = invocationOnMock.getArgument(0, Class.class);
            if (PaymentCashbackUpdate.class.getName().equals(argument.getName()))
                return paymentCashbackUpdateMock;
            else if (TotalTransferCashbackUpdate.class.getName().equals(argument.getName()))
                return totalTransferCashbackUpdateMock;
            else if (PartialTransferCashbackUpdate.class.getName().equals(argument.getName()))
                return partialTransferCashbackUpdateMock;
            else
                throw new IllegalArgumentException();
        })
                .when(beanFactoryMock)
                .getBean(Mockito.any(Class.class));
    }


    @Test
    public void create_OK_payment() {
        CashbackUpdateStrategy serialCashbackUpdateStrategy = cashbackUpdateFactory.create(TransactionType.PAYMENT);
        Assert.assertNotNull(serialCashbackUpdateStrategy);
        Assert.assertTrue(PaymentCashbackUpdate.class.isAssignableFrom(serialCashbackUpdateStrategy.getClass()));
    }

    @Test
    public void create_OK_totalTransfer() {
        CashbackUpdateStrategy parallelCashbackUpdateStrategy = cashbackUpdateFactory.create(TransactionType.TOTAL_TRANSFER);
        Assert.assertNotNull(parallelCashbackUpdateStrategy);
        Assert.assertTrue(TotalTransferCashbackUpdate.class.isAssignableFrom(parallelCashbackUpdateStrategy.getClass()));
    }

    @Test
    public void create_OK_partialTransfer() {
        CashbackUpdateStrategy parallelCashbackUpdateStrategy = cashbackUpdateFactory.create(TransactionType.PARTIAL_TRANSFER);
        Assert.assertNotNull(parallelCashbackUpdateStrategy);
        Assert.assertTrue(PartialTransferCashbackUpdate.class.isAssignableFrom(parallelCashbackUpdateStrategy.getClass()));
    }

}