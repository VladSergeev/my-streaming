package samza;



import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vsergeev on 07.09.2015.
 */
public class TestTask implements StreamTask {
    private final Logger LOGGER = LoggerFactory.getLogger(TestTask.class);
    private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "words");

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        System.out.println("TEST MESSAGE!");
        LOGGER.info("Task processing starts!");


        String message = (String) incomingMessageEnvelope.getMessage();
        messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("kafka","words"),"Do it "+message));
        LOGGER.info("Task processing ends!");
    }
}
