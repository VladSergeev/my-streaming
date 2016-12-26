package my.kafkastreams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Created by vsergeev on 07.06.2016.
 */
public class ProcessorImpl implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new Processor<String, String>() {
            private ProcessorContext context;
            private KeyValueStore<String, String> kvStore;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.context.schedule(1000);
                this.kvStore = (KeyValueStore<String, String>) this.context.getStateStore("Counts");
            }

            @Override
            public void process(String key, String value) {
                String oldValue = this.kvStore.get(key);
                if (oldValue == null) {
                    this.kvStore.put(key, value);
                } else {
                    this.kvStore.put(key,value);
                }

                context.commit();
            }

            @Override
            public void punctuate(long streamTime) {
                KeyValueIterator<String, String> iter = this.kvStore.all();

                while (iter.hasNext()) {
                    KeyValue<String, String> entry = iter.next();

                    System.out.println("[" + entry.key + ", " + entry.value + "]");

                    context.forward(entry.key, entry.value);
                }
            }

            @Override
            public void close() {
                this.kvStore.close();
            }
        };
    }
}
