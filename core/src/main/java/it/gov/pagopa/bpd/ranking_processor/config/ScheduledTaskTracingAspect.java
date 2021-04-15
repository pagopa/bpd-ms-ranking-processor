package it.gov.pagopa.bpd.ranking_processor.config;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.MDC;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.UUID;

@Aspect
@Configuration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ScheduledTaskTracingAspect {

    @Pointcut("@annotation(org.springframework.scheduling.annotation.Scheduled)")
    public void methodAnnotatedWithScheduled() {
    }

//    @Pointcut("execution(* com.mycompany..*(..))")
//    public void atExecutionTimeInMyNamespace() {}

    //    @Around("methodAnnotatedWithScheduled() && atExecutionTimeInMyNamespace()")
    @Around("methodAnnotatedWithScheduled()")
    public Object connectionAdvice(ProceedingJoinPoint joinPoint) throws Throwable {

        String userId = System.getenv("HOSTNAME");
        if (userId == null) {
            userId = System.getenv("COMPUTERNAME");
        }
        MDC.put("user-id", userId);
        MDC.put("request-id", UUID.randomUUID().toString());

        try {
            return joinPoint.proceed();

        } finally {
            MDC.clear();
        }
    }

}
