package transitsims.validation;

import com.doer.Task;
import jakarta.inject.Inject;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/** The Spring version of the interceptor of {@link BeanKindTraced}: an aspect. Copied by the e2e suite (SpringBootApp). */
@Aspect
@Component
public class BeanKindTracedInterceptor {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @Around("@annotation(transitsims.validation.BeanKindTraced) || @within(transitsims.validation.BeanKindTraced)")
    public Object trace(ProceedingJoinPoint joinPoint) throws Throwable {
        Task task = null;
        for (Object argument : joinPoint.getArgs()) {
            if (argument instanceof Task t) {
                task = t;
            }
        }
        String method = joinPoint.getSignature().getName();
        if (task != null) {
            callTrace.add(task, "BeanKindTracedInterceptor before " + method);
        }
        Object result = joinPoint.proceed();
        if (task != null) {
            callTrace.add(task, "BeanKindTracedInterceptor after " + method);
        }
        return result;
    }
}
