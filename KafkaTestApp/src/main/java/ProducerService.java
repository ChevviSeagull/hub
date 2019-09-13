import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerService {
    KafkaProducer<String, String> mProducer;
    final Logger mLogger = LoggerFactory.getLogger(ProducerService.class);



    private Properties producerProperties(String bootstrapServer, String schemaUrl) {
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        props.put("schema.registry.url", schemaUrl);

        return props;
    }

     ProducerService(String bootstrapServer, String schemaUrl){
        Properties props = producerProperties(bootstrapServer, schemaUrl);
         mProducer = new KafkaProducer<>(props);

         mLogger.info("ProducerService initialized");
    }

    void put (String topic, String key, String value) throws ExecutionException, InterruptedException {
         mLogger.info("Put value " + value + ", for key " + key);
        ProducerRecord<String,String> record = new ProducerRecord<>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
            if (e != null){
                mLogger.error("Producing ERROR", e);
                return;
            }

            mLogger.info("Recorded new Metadata");
        });
    }

    void close(){
         mLogger.info("ProducerService closing");
         mProducer.close();
    }
}
