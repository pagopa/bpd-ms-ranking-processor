package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.AwardPeriodRestClient;
import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class RankingProcessorServiceImpl implements RankingProcessorService {

    private final CashbackUpdateService cashbackUpdateService;
    private final RankingUpdateService rankingUpdateService;
    private final AwardPeriodRestClient awardPeriodRestClient;
    private final int cashbackUpdateLimit;
    private final int rankingUpdateLimit;


    @Autowired
    public RankingProcessorServiceImpl(CashbackUpdateService cashbackUpdateService,
                                       RankingUpdateService rankingUpdateService,
                                       AwardPeriodRestClient awardPeriodRestClient,
                                       @Value("${cashback-update.data-extraction.limit}") int cashbackUpdateLimit,
                                       @Value("${ranking-update.data-extraction.limit}") int rankingUpdateLimit) {
        this.cashbackUpdateService = cashbackUpdateService;
        this.rankingUpdateService = rankingUpdateService;
        this.awardPeriodRestClient = awardPeriodRestClient;
        this.cashbackUpdateLimit = cashbackUpdateLimit;
        this.rankingUpdateLimit = rankingUpdateLimit;
    }


    @Scheduled(cron = "${ranking-processor.cron}")
    public void execute() {
        log.info("RankingProcessorServiceImpl.execute start");

        List<AwardPeriod> activeAwardPeriods = awardPeriodRestClient.getActiveAwardPeriods();
        activeAwardPeriods.forEach(awardPeriod -> process(awardPeriod.getAwardPeriodId()));

        log.info("RankingProcessorServiceImpl.execute end");
    }


    @Override
    public void process(Long awardPeriodId) {
        if (log.isTraceEnabled()) {
            log.trace("RankingProcessorServiceImpl.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}", awardPeriodId);
        }

        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }

        updateCashback(awardPeriodId);
        updateRanking(awardPeriodId);
    }


    private void updateCashback(long awardPeriodId) {
        for (WinningTransaction.TransactionType trxType : WinningTransaction.TransactionType.values()) {
            int pageNumber = 0;
            int trxCount;
            do {
                SimplePageRequest pageRequest = SimplePageRequest.of(pageNumber++, cashbackUpdateLimit);
                trxCount = cashbackUpdateService.process(awardPeriodId, trxType, pageRequest);
            } while (cashbackUpdateLimit == trxCount);
        }
    }


    private void updateRanking(long awardPeriodId) {//TODO: manage tied between each chunks
        int pageNumber = 0;
        int trxCount;
        MutableInt lastAssignedRanking = new MutableInt(0);
        do {
            SimplePageRequest pageRequest = SimplePageRequest.of(pageNumber++, rankingUpdateLimit);
            trxCount = rankingUpdateService.process(awardPeriodId, lastAssignedRanking, pageRequest);
        } while (rankingUpdateLimit == trxCount);
    }

}

