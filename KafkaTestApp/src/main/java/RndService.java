import java.util.Random;

public class RndService {

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
}
