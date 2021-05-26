package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.ActiveUserWinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.util.DaoHelper;
import it.gov.pagopa.bpd.ranking_processor.service.cashback.CashbackUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao.RankingProcess.DAILY_PAYMENT_LIMIT;

@Slf4j
@Service
public class DailyPaymentLimitDetectorServiceImpl implements DailyPaymentLimitDetectorService {//TODO test

    private final CitizenRankingDao citizenRankingDao;
    private final WinningTransactionDao winningTransactionDao;
    private final DailyPaymentLimitCleanUpService dailyPaymentLimitCleanUpService;
    private final LocalDate eventDate;
    private final int validPayments;

    @Autowired
    public DailyPaymentLimitDetectorServiceImpl(CitizenRankingDao citizenRankingDao, WinningTransactionDao winningTransactionDao, DailyPaymentLimitCleanUpService dailyPaymentLimitCleanUpService, @Value("${check.daily-payment-limit.event-date:2021-06-01}") LocalDate eventDate, @Value("${check.daily-payment-limit.valid-payment:5}") int validPayments) {
        this.citizenRankingDao = citizenRankingDao;
        this.winningTransactionDao = winningTransactionDao;
        this.dailyPaymentLimitCleanUpService = dailyPaymentLimitCleanUpService;
        this.eventDate = eventDate;
        this.validPayments = validPayments;
    }


    @Override
    public void checkPaymentLimit(Long awardPeriodId) {
        if(citizenRankingDao.getWorkerCount(DAILY_PAYMENT_LIMIT) > 0){
            log.info("Check payment limit already started");
            return;
        }
        registerWorker(DAILY_PAYMENT_LIMIT,true);

        try{
            List<ActiveUserWinningTransaction> activeUserWinningTransactionList = winningTransactionDao.findActiveUsersSinceLastDetector(awardPeriodId);
            if(!CollectionUtils.isEmpty(activeUserWinningTransactionList)){
                OffsetDateTime maxInsertDate = null;
                Map<String, List<Triple<LocalDate, String, List<WinningTransaction>>>> validPaymentsMap = new HashMap<>();
                for (ActiveUserWinningTransaction activeUserWinningTransaction : activeUserWinningTransactionList) {
                    if(eventDate.isAfter(activeUserWinningTransaction.getTrxDate())){
                       continue;
                    }
                    List<WinningTransaction> topValidWinningTransactions = winningTransactionDao.findTopValidWinningTransactions(awardPeriodId, validPayments, activeUserWinningTransaction.getFiscalCode(), activeUserWinningTransaction.getMerchantId(), activeUserWinningTransaction.getTrxDate());
                    if(!CollectionUtils.isEmpty(topValidWinningTransactions)){
                        validPaymentsMap.computeIfAbsent(activeUserWinningTransaction.getFiscalCode(), p-> new ArrayList<>());
                        validPaymentsMap.get(activeUserWinningTransaction.getFiscalCode()).add(Triple.of(activeUserWinningTransaction.getTrxDate(), activeUserWinningTransaction.getMerchantId(), topValidWinningTransactions));
                    }
                    if(null == maxInsertDate || maxInsertDate.isBefore(activeUserWinningTransaction.getInsertDate())){
                        maxInsertDate = activeUserWinningTransaction.getInsertDate();
                    }
                }
                dailyPaymentLimitCleanUpService.cleanUpService(validPaymentsMap, awardPeriodId);

                winningTransactionDao.updateDetectorLastExecution(maxInsertDate);
            }
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            unregisterWorker(DAILY_PAYMENT_LIMIT);
            throw e;
        }

        unregisterWorker(DAILY_PAYMENT_LIMIT);

    }

    private void registerWorker(CitizenRankingDao.RankingProcess process, boolean exclusiveLock) {
        int affectedRow = citizenRankingDao.registerWorker(process, exclusiveLock);
        checkError(affectedRow, "register", process);
    }

    private void unregisterWorker(CitizenRankingDao.RankingProcess process) {
        int affectedRow = citizenRankingDao.unregisterWorker(process);
        checkError(affectedRow, "unregister", process);
    }

    private void checkError(int affectedRow, String action, CitizenRankingDao.RankingProcess process) {
        if (DaoHelper.isStatementResultKO.test(affectedRow)) {
            String message = String.format("Failed to %s worker to process %s", action, process);
            log.error(message);
            throw new CashbackUpdateException(message);
        }
    }
}
