package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction.TransactionType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;

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
        ObjectProvider paymentCashbackUpdateObjectProviderMock = Mockito.mock(ObjectProvider.class);
        BDDMockito.doReturn(paymentCashbackUpdateMock)
                .when(paymentCashbackUpdateObjectProviderMock)
                .getIfAvailable();

        ObjectProvider totalTransferCashbackUpdateObjectProviderMock = Mockito.mock(ObjectProvider.class);
        BDDMockito.doReturn(totalTransferCashbackUpdateMock)
                .when(totalTransferCashbackUpdateObjectProviderMock)
                .getIfAvailable();

        ObjectProvider partialTransferCashbackUpdateObjectProviderMock = Mockito.mock(ObjectProvider.class);
        BDDMockito.doReturn(partialTransferCashbackUpdateMock)
                .when(partialTransferCashbackUpdateObjectProviderMock)
                .getIfAvailable();

        BDDMockito.doAnswer(invocationOnMock -> {
            Class argument = invocationOnMock.getArgument(0, Class.class);
            if (PaymentCashbackUpdate.class.getName().equals(argument.getName()))
                return paymentCashbackUpdateObjectProviderMock;
            else if (TotalTransferCashbackUpdate.class.getName().equals(argument.getName()))
                return totalTransferCashbackUpdateObjectProviderMock;
            else if (PartialTransferCashbackUpdate.class.getName().equals(argument.getName()))
                return partialTransferCashbackUpdateObjectProviderMock;
            else
                throw new IllegalArgumentException();
        })
                .when(beanFactoryMock)
                .getBeanProvider(Mockito.any(Class.class));
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