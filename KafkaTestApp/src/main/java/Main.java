import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        //hardcode topic + kafkaServer
        String server = ":9092";
        String topic = "input";

        if (args.length != 2) {
            System.out.println("Please provide command line argument: schemaRegistryUrl");
            System.exit(-1);
        }
        String schemaUrl = args[1];

        ProducerService producer = new ProducerService(server,schemaUrl);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            producer.put(topic, RndService.generateString(100), RndService.generateFloat());
            if (reader.readLine()!= null){
                break;
            }
        }
        producer.close();
        reader.close();
    }

}
