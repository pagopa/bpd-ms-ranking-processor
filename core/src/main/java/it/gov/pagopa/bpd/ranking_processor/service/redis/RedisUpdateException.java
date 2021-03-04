package it.gov.pagopa.bpd.ranking_processor.service.redis;

public class RedisUpdateException extends RuntimeException {

    public RedisUpdateException(String message) {
        super(message);
    }
}
