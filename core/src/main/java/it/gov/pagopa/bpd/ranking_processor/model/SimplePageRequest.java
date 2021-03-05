package it.gov.pagopa.bpd.ranking_processor.model;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SimplePageRequest {

    private final int page;
    private final int size;

    private SimplePageRequest(int page, int size) {
        this.page = page;
        this.size = size;
    }

    /**
     * Creates a new {@link SimplePageRequest}.
     *
     * @param page zero-based page index.
     * @param size the size of the page to be returned.
     */
    public static SimplePageRequest of(int page, int size) {
        return new SimplePageRequest(page, size);
    }

}
