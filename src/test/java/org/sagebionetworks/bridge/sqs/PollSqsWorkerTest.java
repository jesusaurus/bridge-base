package org.sagebionetworks.bridge.sqs;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.sqs.model.Message;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PollSqsWorkerTest {
    private static final String SQS_MESSAGE_BAD_REQUEST = "bad-request-message";
    private static final String SQS_MESSAGE_ERROR = "error-message";
    private static final String SQS_MESSAGE_RETRYABLE_ERROR = "retryable-error-message";
    private static final String SQS_MESSAGE_SUCCESS = "success-message";
    private static final String SQS_RECEIPT_BAD_REQUEST = "bad-request-receipt-handle";
    private static final String SQS_RECEIPT_ERROR = "error-receipt-handle";
    private static final String SQS_RECEIPT_RETRYABLE_ERROR = "retryable-error-receipt-handle";
    private static final String SQS_RECEIPT_SUCCESS = "success-receipt-handle";
    private static final String SQS_QUEUE_URL = "dummy-queue-url";

    @Mock
    private SqsHelper mockSqsHelper;

    // Spy so we can mock out shouldKeepRunning.
    @InjectMocks
    @Spy
    private PollSqsWorker worker;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);

        worker.setQueueUrl(SQS_QUEUE_URL);
        worker.setSleepTimeMillis(0);
    }

    @Test
    public void test() {
        // Test strategy - 4 iterations:
        // 1. no message
        // 2. exception
        // 3. success
        // 4. bad request
        // 5. retryable exception

        // mock SQS helper
        Message message2 = new Message().withBody(SQS_MESSAGE_ERROR).withReceiptHandle(SQS_RECEIPT_ERROR);
        Message message3 = new Message().withBody(SQS_MESSAGE_SUCCESS).withReceiptHandle(SQS_RECEIPT_SUCCESS);
        Message message4 = new Message().withBody(SQS_MESSAGE_BAD_REQUEST).withReceiptHandle(
                SQS_RECEIPT_BAD_REQUEST);
        Message message5 = new Message().withBody(SQS_MESSAGE_RETRYABLE_ERROR).withReceiptHandle(
                SQS_RECEIPT_RETRYABLE_ERROR);
        when(mockSqsHelper.poll(SQS_QUEUE_URL)).thenReturn(null, message2, message3, message4, message5);

        // test callback
        Set<String> receivedMessageSet = new HashSet<>();
        PollSqsCallback testCallback = messageBody -> {
            receivedMessageSet.add(messageBody);
            switch (messageBody) {
                case SQS_MESSAGE_ERROR:
                    throw new TestException();
                case SQS_MESSAGE_BAD_REQUEST:
                    throw new PollSqsWorkerBadRequestException();
                case SQS_MESSAGE_RETRYABLE_ERROR:
                    throw new PollSqsWorkerRetryableException();
                default:
                    // Do nothing.
            }
        };
        worker.setCallback(testCallback);

        // spy shouldKeepRunning() - 5 iterations
        doReturn(true).doReturn(true).doReturn(true).doReturn(true).doReturn(true)
                .doReturn(false).when(worker).shouldKeepRunning();

        // execute
        worker.run();

        // validate - callback received both messages
        assertEquals(receivedMessageSet.size(), 4);
        assertTrue(receivedMessageSet.contains(SQS_MESSAGE_ERROR));
        assertTrue(receivedMessageSet.contains(SQS_MESSAGE_SUCCESS));
        assertTrue(receivedMessageSet.contains(SQS_MESSAGE_BAD_REQUEST));
        assertTrue(receivedMessageSet.contains(SQS_MESSAGE_RETRYABLE_ERROR));

        // validate - success message and bad request message are deleted; error message is not
        verify(mockSqsHelper).deleteMessage(SQS_QUEUE_URL, SQS_RECEIPT_SUCCESS);
        verify(mockSqsHelper, never()).deleteMessage(SQS_QUEUE_URL, SQS_RECEIPT_ERROR);
        verify(mockSqsHelper).deleteMessage(SQS_QUEUE_URL, SQS_RECEIPT_BAD_REQUEST);
        verify(mockSqsHelper, never()).deleteMessage(SQS_QUEUE_URL, SQS_RECEIPT_RETRYABLE_ERROR);
    }

    @Test
    public void testWithExecutorService() throws Exception {
        // Mock SQS helper.
        Message message = new Message().withBody(SQS_MESSAGE_SUCCESS).withReceiptHandle(SQS_RECEIPT_SUCCESS);
        when(mockSqsHelper.poll(SQS_QUEUE_URL)).thenReturn(message);

        // Mock callback that does nothing. Later, we check to see that it was called.
        PollSqsCallback mockCallback = mock(PollSqsCallback.class);
        worker.setCallback(mockCallback);

        // Spy shouldKeepRunning() - 1 iterations.
        doReturn(true).doReturn(false).when(worker).shouldKeepRunning();

        // Mock ExecutorService. For the purposes of this test, it just calls through to the submitted runnable.
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            invocation.getArgumentAt(0, Runnable.class).run();
            return null;
        }).when(mockExecutorService).execute(any());
        worker.setExecutorService(mockExecutorService);

        // Execute.
        worker.run();

        // Validate callback is called and the SQS message is deleted.
        verify(mockCallback).callback(SQS_MESSAGE_SUCCESS);
        verify(mockSqsHelper).deleteMessage(SQS_QUEUE_URL, SQS_RECEIPT_SUCCESS);
    }

    @SuppressWarnings("serial")
    private static class TestException extends Exception {
    }
}
