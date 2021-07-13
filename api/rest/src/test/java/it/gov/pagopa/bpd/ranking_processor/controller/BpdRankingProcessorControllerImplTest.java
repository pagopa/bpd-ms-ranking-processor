package it.gov.pagopa.bpd.ranking_processor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.sia.meda.DummyConfiguration;
import eu.sia.meda.error.config.LocalErrorConfig;
import eu.sia.meda.error.handler.MedaExceptionHandler;
import eu.sia.meda.error.service.impl.LocalErrorManagerServiceImpl;
import it.gov.pagopa.bpd.ranking_processor.controller.model.RankingProcessorDto;
import it.gov.pagopa.bpd.ranking_processor.service.RankingProcessorService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import javax.annotation.PostConstruct;

@RunWith(SpringRunner.class)
@WebMvcTest(value = {BpdRankingProcessorControllerImpl.class}, excludeAutoConfiguration = SecurityAutoConfiguration.class)
@ContextConfiguration(classes = {
        BpdRankingProcessorControllerImpl.class,
        DummyConfiguration.class,
        MedaExceptionHandler.class,
        LocalErrorManagerServiceImpl.class
})
@Import(LocalErrorConfig.class)
@TestPropertySource(properties = {
        "error-manager.enabled=true",
        "spring.application.name=bpd-ms-ranking-processor-api-rest"
})
public class BpdRankingProcessorControllerImplTest {

    private static final String URL_TEMPLATE_PREFIX = "/bpd/ranking-processor";

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private RankingProcessorService rankingProcessorService;

    @PostConstruct
    public void initMocks() {
        BDDMockito.doAnswer(invocationOnMock -> {
            Long awardPeriodId = invocationOnMock.getArgument(0, Long.class);
            if (awardPeriodId == -1L)
                throw new RuntimeException("Test Error");
            return null;
        }).when(rankingProcessorService)
                .process(Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void execute_OK() throws Exception {
        RankingProcessorDto request = new RankingProcessorDto();
        request.setAwardPeriodId(1L);

        mvc.perform(MockMvcRequestBuilders
                .post(URL_TEMPLATE_PREFIX + "/")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is2xxSuccessful())
                .andReturn();
    }

    @Test
    public void execute_KOProcess() throws Exception {
        RankingProcessorDto request = new RankingProcessorDto();
        request.setAwardPeriodId(-1L);

        mvc.perform(MockMvcRequestBuilders
                .post(URL_TEMPLATE_PREFIX + "/")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andReturn();
    }

    @Test
    public void execute_KOInvalidRequest() throws Exception {
        mvc.perform(MockMvcRequestBuilders
                .post(URL_TEMPLATE_PREFIX + "/")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(null)))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError())
                .andReturn();
    }

    @Test
    public void execute_KOInvalidRequestField() throws Exception {
        RankingProcessorDto request = new RankingProcessorDto();
        request.setAwardPeriodId(null);

        mvc.perform(MockMvcRequestBuilders
                .post(URL_TEMPLATE_PREFIX + "/")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError())
                .andReturn();
    }

}