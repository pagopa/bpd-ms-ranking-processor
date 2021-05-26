package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;

@Slf4j
@Service
public class DailyPaymentLimitUserCleanUpServiceImpl implements DailyPaymentLimitUserCleanUpService {//TODO test

    private final WinningTransactionDao winningTransactionDao;
    private final CitizenRankingDao citizenRankingDao;

    @Autowired
    public DailyPaymentLimitUserCleanUpServiceImpl(WinningTransactionDao winningTransactionDao, CitizenRankingDao citizenRankingDao) {
        this.winningTransactionDao = winningTransactionDao;
        this.citizenRankingDao = citizenRankingDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanUpUserService(String fiscalCode, List<Triple<LocalDate, String, List<WinningTransaction>>> validPaymentsList, Long awardPeriodId) {
        for (Triple<LocalDate, String, List<WinningTransaction>> validDayPayments : validPaymentsList) {
            winningTransactionDao.updateInvalidateTransactions(fiscalCode, validDayPayments.getLeft(), validDayPayments.getMiddle(), awardPeriodId);
            winningTransactionDao.updateSetValidTransactions(validDayPayments.getRight());
        }
        citizenRankingDao.resetCashback(fiscalCode, awardPeriodId);

        winningTransactionDao.updateUserTransactionsElab(fiscalCode, awardPeriodId);

        //TODO updateCashback(fiscalCode, awardPeriodId) ricalcolo cashback per il codiceFiscale nel periodo indicato
    }
}
