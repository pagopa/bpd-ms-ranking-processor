package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class RankingProcessorServiceImplTest {

    private static FindActiveResult findActiveResult;

    private final RankingProcessorServiceImpl rankingProcessorService;
    private final AwardPeriodRestClient restClientMock;
    private final List<RankingSubProcessCommand> subProcessesMock;

    public RankingProcessorServiceImplTest() {
        restClientMock = Mockito.mock(AwardPeriodRestClient.class);
        BDDMockito.when(restClientMock.getActiveAwardPeriods())
                .thenAnswer(invocationOnMock -> {
                    ArrayList<AwardPeriod> awardPeriods = new ArrayList<>();
                    int loop = findActiveResult.getValue();
                    for (int i = 0; i < loop; i++) {
                        awardPeriods.add(buildAwardPeriod(2));
                    }
                    return awardPeriods;
                });
        BDDMockito.when(restClientMock.findById(1))
                .thenReturn(buildAwardPeriod(1));

        subProcessesMock = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            subProcessesMock.add(Mockito.mock(RankingSubProcessCommand.class));
        }

        rankingProcessorService = new RankingProcessorServiceImpl(restClientMock, subProcessesMock);
    }

    private static AwardPeriod buildAwardPeriod(int bias) {
        return AwardPeriod.builder()
                .awardPeriodId((long) bias)
                .minPosition(1000L)
                .maxPeriodCashback(150L)
                .maxTransactionEvaluated(150L)
                .cashbackPercentage(10)
                .build();
    }

    @Test
    public void execute_OkNoActivePeriods() {
        findActiveResult = FindActiveResult.ZERO;

        rankingProcessorService.execute();

        verify(restClientMock, only()).getActiveAwardPeriods();
        subProcessesMock.forEach(command -> verifyZeroInteractions(command));
    }

    @Test
    public void execute_OkSingleActivePeriods() {
        findActiveResult = FindActiveResult.ONE;

        rankingProcessorService.execute();

        verify(restClientMock, only()).getActiveAwardPeriods();
        subProcessesMock.forEach(command -> verify(command, only()).execute(any(AwardPeriod.class)));
    }

    @Test
    public void execute_OkMoreActivePeriods() {
        findActiveResult = FindActiveResult.TEN;

        rankingProcessorService.execute();

        verify(restClientMock, only()).getActiveAwardPeriods();
        subProcessesMock.forEach(command -> verify(command, times(findActiveResult.getValue())).execute(any(AwardPeriod.class)));
    }

    @Test
    public void process_Ok() {
        rankingProcessorService.process(1L);

//        verify(restClientMock, only()).findById(1);//TODO decomment me
        subProcessesMock.forEach(command -> verify(command, only()).execute(any(AwardPeriod.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void process_KoNullAwardPeriod() {
        try {
            rankingProcessorService.process(null);
        } catch (Exception e) {
            verifyZeroInteractions(restClientMock);
            subProcessesMock.forEach(command -> verifyZeroInteractions(command));
            throw e;
        }
    }

    enum FindActiveResult {
        ZERO(0),
        ONE(1),
        TEN(10);

        private int value;

        FindActiveResult(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}