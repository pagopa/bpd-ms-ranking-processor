package it.gov.pagopa.bpd.ranking_processor.service.cashback.strategy;

import it.gov.pagopa.bpd.ranking_processor.connector.award_period.model.AwardPeriod;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.WinningTransaction;
import it.gov.pagopa.bpd.ranking_processor.model.SimplePageRequest;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import static it.gov.pagopa.bpd.ranking_processor.connector.jdbc.WinningTransactionDao.FIND_TRX_TO_PROCESS_PAGEABLE_SORT;

/**
 * Implementation of {@link CashbackUpdateStrategyTemplate} to handle total transfer
 */
@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Conditional(CashbackUpdateTotalTransferEnabledCondition.class)
class TotalTransferCashbackUpdate extends CashbackUpdateStrategyTemplate {

    private final int dataExtractionLimit;
    private final Period maxDepth;
    private final OffsetDateTime startProcess;


    @Autowired
    public TotalTransferCashbackUpdate(WinningTransactionDao winningTransactionDao,
                                       CitizenRankingDao citizenRankingDao,
                                       BeanFactory beanFactory,
                                       @Value("${cashback-update.total-transfer.data-extraction.limit}") int dataExtractionLimit,
                                       @Value("${cashback-update.total-transfer.max-depth}") Period maxDepth) {
        super(winningTransactionDao,
                citizenRankingDao,
                beanFactory.getBean(CommonAggregator.class));
        this.dataExtractionLimit = dataExtractionLimit;
        this.maxDepth = maxDepth;
        this.startProcess = OffsetDateTime.now();
    }


    @Override
    public int getDataExtractionLimit() {
        return dataExtractionLimit;
    }


    @Override
    protected List<WinningTransaction> retrieveTransactions(long awardPeriodId, Pageable pageable) {
        Pageable pageRequest = PageRequest.of(pageable.getPageNumber(),
                pageable.getPageSize(),
                FIND_TRX_TO_PROCESS_PAGEABLE_SORT);
        WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
        filterCriteria.setAwardPeriodId(awardPeriodId);
        filterCriteria.setUpdateDate(startProcess);

        return winningTransactionDao.findTransferToProcess(filterCriteria, pageRequest);
    }


    @Override
    @Transactional("chainedTransactionManager")
    public int process(AwardPeriod awardPeriod, SimplePageRequest simplePageRequest) {
        if (log.isTraceEnabled()) {
            log.trace("CashbackUpdateStrategyTemplate.process");
        }
        if (log.isDebugEnabled()) {
            log.debug("awardPeriodId = {}, simplePageRequest = {}", awardPeriod, simplePageRequest);
        }

        Pageable pageRequest = PageRequest.of(simplePageRequest.getPage(), simplePageRequest.getSize());
        List<WinningTransaction> transactions = retrieveTransactions(awardPeriod.getAwardPeriodId(), pageRequest);

        List<WinningTransaction> unrelatedTransfer = new ArrayList<>();
        List<WinningTransaction> relatedPartialTransfer = new ArrayList<>();
        List<WinningTransaction> relatedTotalTransfer = new ArrayList<>();
        List<WinningTransaction> oldTransfer = new ArrayList<>();

        OffsetDateTime max = OffsetDateTime.now().minus(maxDepth);

        for (WinningTransaction transferTrx : transactions) {
            if (transferTrx.getInsertDate().isBefore(max)) {
                oldTransfer.add(transferTrx);

            } else {
                transferTrx.setUpdateDate(startProcess);
                transferTrx.setUpdateUser(RankingProcessorService.PROCESS_NAME);

                WinningTransaction paymentTrx;
                WinningTransaction.FilterCriteria filterCriteria = new WinningTransaction.FilterCriteria();
                filterCriteria.setAwardPeriodId(awardPeriod.getAwardPeriodId());
                filterCriteria.setHpan(transferTrx.getHpan());
                filterCriteria.setAcquirerCode(transferTrx.getAcquirerCode());
                filterCriteria.setAcquirerId(transferTrx.getAcquirerId());
                if (StringUtils.isBlank(transferTrx.getCorrelationId())) {
                    filterCriteria.setAmount(transferTrx.getAmount());
                    filterCriteria.setMerchantId(transferTrx.getMerchantId());
                    filterCriteria.setTerminalId(transferTrx.getTerminalId());
                    paymentTrx = winningTransactionDao.findPaymentTrxWithoutCorrelationId(filterCriteria);
                } else {
                    filterCriteria.setCorrelationId(transferTrx.getCorrelationId());
                    try {
                        paymentTrx = winningTransactionDao.findPaymentTrxWithCorrelationId(filterCriteria);
                    } catch (IncorrectResultSizeDataAccessException e) {
                        log.warn(String.format("Failed to match transfer with correlation_id '%s': %s",
                                transferTrx.getCorrelationId(),
                                e.getMessage()));
                        transferTrx.setParked(true);
                        paymentTrx = null;
                    }
                }

                if (paymentTrx == null) {
                    unrelatedTransfer.add(transferTrx);

                } else {

                    if (transferTrx.getAmount().equals(paymentTrx.getAmount())) {
                        relatedTotalTransfer.add(transferTrx);

                    } else {
                        relatedPartialTransfer.add(transferTrx);
                    }
                }
            }
        }

        List<CitizenRanking> rankings = aggregate(awardPeriod, relatedTotalTransfer);
        updateCashback(rankings);

        if (!relatedTotalTransfer.isEmpty()) {
            int[] affectedRows = winningTransactionDao.updateProcessedTransaction(relatedTotalTransfer);
            checkErrors(relatedTotalTransfer.size(), affectedRows, "updateProcessedTransaction");
        }

        if (!relatedPartialTransfer.isEmpty()) {
            int[] affectedRows = winningTransactionDao.updateUnprocessedPartialTransfer(relatedPartialTransfer);
            checkErrors(relatedPartialTransfer.size(), affectedRows, "updateUnprocessedPartialTransfer");
        }

        if (!unrelatedTransfer.isEmpty()) {
            int[] affectedRows = winningTransactionDao.updateUnrelatedTransfer(unrelatedTransfer);
            checkErrors(unrelatedTransfer.size(), affectedRows, "updateUnrelatedTransfer");
        }

        if (!oldTransfer.isEmpty()) {
            int[] affectedRows = winningTransactionDao.deleteTransfer(oldTransfer);
            checkErrors(oldTransfer.size(), affectedRows, "deleteTransfer");
        }

        return transactions.size();
    }

}
