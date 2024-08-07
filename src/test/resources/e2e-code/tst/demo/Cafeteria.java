package tst.demo;

import com.doer.AcceptStatus;
import com.doer.DoerConcurrency;
import com.doer.OnException;
import com.doer.Task;
import jakarta.enterprise.context.Dependent;

@DoerConcurrency(10)
@Dependent
public class Cafeteria {
    @AcceptStatus("Customer wants to make an order")
    public void recordCustomersOrder(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Order accepted");
    }

    @AcceptStatus("Want a coffee")
    public void visitCafeteria(Task task) throws Exception {
        task.setStatus("Customer is ready to pay");
    }

    // Jump between ConcurrencyDomains class-method-class
    @AcceptStatus("Need bubblegum")
    public void selectBubbleGum(Task task) {
        task.setStatus("Selected bubblegum");
    }

    @DoerConcurrency(2)
    @AcceptStatus("Selected bubblegum")
    public void payForBubbleGum(Task task) {
        task.setStatus("Bubblegum payed");
    }

    @AcceptStatus("Bubblegum payed")
    public void sayGoodBay(Task task) {
        task.setStatus("Sayed goodbay");
    }

    // Delayed method
    @AcceptStatus(value = "Receipt print started", delay = "2s")
    public void waitReceiptIsPrinted(Task task) {
        task.setStatus("Receipt printed");
    }

    // Error handling and retries
    @AcceptStatus("Should send email")
    public void sendEmailToCustomer(Task task, Car car) throws Exception {
        throw new Exception("Email sending failed");
    }

    @AcceptStatus("Should check email")
    @OnException(retry = "every 2 sec during 10 seconds", setStatus = "Email check failed")
    public void checkEmail(Task task, Car car) {
        throw new RuntimeException("Check email failed");
    }
}
