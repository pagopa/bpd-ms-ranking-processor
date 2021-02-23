package it.gov.pagopa.bpd.ranking_processor.controller;

import eu.sia.meda.core.controller.StatelessController;
import it.gov.pagopa.bpd.ranking_processor.controller.model.RankingProcessorDto;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
class BpdRankingProcessorControllerImpl extends StatelessController implements BpdRankingProcessorController {

    private final RankingProcessorService rankingProcessorService;


    @Autowired
    public BpdRankingProcessorControllerImpl(RankingProcessorService rankingProcessorService) {
        if (log.isTraceEnabled()) {
            log.trace("BpdRankingProcessorControllerImpl.BpdRankingProcessorControllerImpl");
        }
        if (log.isDebugEnabled()) {
            log.debug("rankingProcessorService = {}", rankingProcessorService);
        }

        this.rankingProcessorService = rankingProcessorService;
    }


    @Override
    public void execute(RankingProcessorDto rankingProcessorDto) {
        log.info("BpdRankingProcessorControllerImpl.execute start");
        if (log.isDebugEnabled()) {
            log.debug("rankingProcessorDto = {}", rankingProcessorDto);
        }

        rankingProcessorService.process(rankingProcessorDto.getAwardPeriodId());

        log.info("BpdRankingProcessorControllerImpl.execute end");
    }

}
