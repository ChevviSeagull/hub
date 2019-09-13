import java.util.Random;
import java.util.concurrent.ExecutionException;

public class RndService implements Runnable{

    public static Random rnd = new Random();

    public static String generateString(int length) {
        String characters = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890";
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rnd.nextInt(characters.length()));
        }
        return new String(text);
    }

    public static String generateFloat() {
        String s = String.valueOf(rnd.nextFloat());
        return  s;
    }

    public void run(ProducerService producer, String topic) throws ExecutionException, InterruptedException{
        producer.put(topic, RndService.generateString(100), RndService.generateFloat());
    }

    @Override
    public void run() {

    }
}
