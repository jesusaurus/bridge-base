package org.sagebionetworks.bridge.sqs;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.sqs.model.Message;
import org.testng.annotations.Test;

public class PollSqsWorkerTest {
    @Test
    public void test() {
        // Test strategy - 4 iterations:
        // 1. no message
        // 2. exception
        // 3. success
        // 4. bad request

        // mock SQS helper
        Message message2 = new Message().withBody("error-message").withReceiptHandle("error-receipt-handle");
        Message message3 = new Message().withBody("success-message").withReceiptHandle("success-receipt-handle");
        Message message4 = new Message().withBody("bad-request-message").withReceiptHandle(
                "bad-request-receipt-handle");

        SqsHelper mockSqsHelper = mock(SqsHelper.class);
        when(mockSqsHelper.poll("dummy-queue-url")).thenReturn(null, message2, message3, message4);

        // test callback
        Set<String> receivedMessageSet = new HashSet<>();
        PollSqsCallback testCallback = messageBody -> {
            receivedMessageSet.add(messageBody);
            if (messageBody.equals("error-message")) {
                throw new TestException();
            } else if (messageBody.equals("bad-request-message")) {
                throw new PollSqsWorkerBadRequestException();
            }
        };

        // set up test worker - spy so we can mock out shouldKeepRunning
        PollSqsWorker worker = spy(new PollSqsWorker());
        worker.setCallback(testCallback);
        worker.setQueueUrl("dummy-queue-url");
        worker.setSleepTimeMillis(0);
        worker.setSqsHelper(mockSqsHelper);

        // spy shouldKeepRunning() - 3 iterations
        doReturn(true).doReturn(true).doReturn(true).doReturn(true).doReturn(false).when(worker).shouldKeepRunning();

        // execute
        worker.run();

        // validate - callback received both messages
        assertEquals(receivedMessageSet.size(), 3);
        assertTrue(receivedMessageSet.contains("error-message"));
        assertTrue(receivedMessageSet.contains("success-message"));
        assertTrue(receivedMessageSet.contains("bad-request-message"));

        // validate - success message and bad request message are deleted; error message is not
        verify(mockSqsHelper).deleteMessage("dummy-queue-url", "success-receipt-handle");
        verify(mockSqsHelper, never()).deleteMessage("dummy-queue-url", "error-receipt-handle");
        verify(mockSqsHelper).deleteMessage("dummy-queue-url", "bad-request-receipt-handle");
    }

    @SuppressWarnings("serial")
    private static class TestException extends Exception {
    }
}
