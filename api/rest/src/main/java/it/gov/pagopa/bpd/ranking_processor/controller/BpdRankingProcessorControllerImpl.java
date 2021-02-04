package it.gov.pagopa.bpd.ranking_processor.controller;

import eu.sia.meda.core.controller.StatelessController;
import it.gov.pagopa.bpd.ranking_processor.assembler.DummyResourceAssembler;
import it.gov.pagopa.bpd.ranking_processor.command.DummyCommand;
import it.gov.pagopa.bpd.ranking_processor.model.dto.DummyDTO;
import it.gov.pagopa.bpd.ranking_processor.model.resource.DummyResource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
class BpdRankingProcessorControllerImpl extends StatelessController implements BpdRankingProcessorController {

    private final BeanFactory beanFactory;
    private final DummyResourceAssembler dummyResourceAssembler;

    @Autowired
    public BpdRankingProcessorControllerImpl(BeanFactory beanFactory, DummyResourceAssembler dummyResourceAssembler) {
        this.beanFactory = beanFactory;
        this.dummyResourceAssembler = dummyResourceAssembler;
    }

    @Override
    public DummyResource test(DummyDTO request) { //FIXME: remove me (created as archetype test)
        log.info(request.toString());

        DummyResource result = null;
        try {
            result = dummyResourceAssembler.toResource(beanFactory.getBean(DummyCommand.class, request.getMessage()).execute());
        } catch (Exception e) {
            log.error("Something gone wrong", e);
        }

        return result;
    }

}
