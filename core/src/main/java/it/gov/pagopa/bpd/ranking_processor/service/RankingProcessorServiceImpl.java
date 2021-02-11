package it.gov.pagopa.bpd.ranking_processor.service;

import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.CitizenRankingDao;
import it.gov.pagopa.bpd.ranking_processor.connector.jdbc.model.CitizenRanking;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;


public class RankingProcessorServiceImpl implements RankingProcessorService {

    private final CitizenRankingDao citizenRankingDao;

    @Autowired
    public RankingProcessorServiceImpl(CitizenRankingDao citizenRankingDao) {
        this.citizenRankingDao = citizenRankingDao;
    }

    @Override
    public void process(@NotNull Long awardPeriodId, @NotNull Pageable pageable) {
        if (awardPeriodId == null) {
            throw new IllegalArgumentException("awardPeriodId can not be null");
        }
        Pageable pageRequest;
        if (Sort.unsorted().equals(pageable.getSort())) {
            pageRequest = PageRequest.of(pageable.getPageSize(),
                    pageable.getPageNumber(),
                    CitizenRankingDao.SORT_BY_TRX_NUM_DESC);
        } else {
            pageRequest = PageRequest.of(pageable.getPageSize(), pageable.getPageNumber());
        }

        List<CitizenRanking> citizenRankings = citizenRankingDao.findAll(awardPeriodId, pageRequest);

        ConcurrentMap<Long, List<CitizenRanking>> tieMap = citizenRankings.stream()
                .parallel()
                .collect(Collectors.groupingByConcurrent(CitizenRanking::getTransactionNumber));


    }

    private void tieBreak() {

    }
}
