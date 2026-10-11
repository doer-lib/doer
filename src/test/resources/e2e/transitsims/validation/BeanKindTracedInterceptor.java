package transitsims.validation;

import com.doer.Task;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

/** Writes to {@link CallTrace} before and after a method with {@link BeanKindTraced} that takes a Task. */
@BeanKindTraced
@Interceptor
@Priority(Interceptor.Priority.APPLICATION)
public class BeanKindTracedInterceptor {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @AroundInvoke
    public Object trace(InvocationContext context) throws Exception {
        Task task = null;
        for (Object parameter : context.getParameters()) {
            if (parameter instanceof Task t) {
                task = t;
            }
        }
        String method = context.getMethod().getName();
        if (task != null) {
            callTrace.add(task, "BeanKindTracedInterceptor before " + method);
        }
        Object result = context.proceed();
        if (task != null) {
            callTrace.add(task, "BeanKindTracedInterceptor after " + method);
        }
        return result;
    }
}
