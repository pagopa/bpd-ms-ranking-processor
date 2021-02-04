package it.gov.pagopa.bpd.ranking_processor.assembler;

import it.gov.pagopa.bpd.ranking_processor.model.DummyModel;
import it.gov.pagopa.bpd.ranking_processor.model.resource.DummyResource;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
//FIXME: remove me (created as archetype test)
public class DummyResourceAssembler {

    public DummyResource toResource(DummyModel model) {
        DummyResource resource = null;

        if (model != null) {
            resource = new DummyResource();
            BeanUtils.copyProperties(model, resource);
        }

        return resource;
    }

}
