import java.util.concurrent.ThreadLocalRandom;

public final class RandomGenerator {

    private static final String CHAR_LIST =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    public String getRandomString(int minLength, int maxLength) {
        int randomStringLength = getRandomIntegerBetween(minLength, maxLength);
        StringBuilder randomString = new StringBuilder();
        for(int i=0; i<randomStringLength; i++){
            int number = getRandomIntegerBetween(0, CHAR_LIST.length() - 1);
            char character = CHAR_LIST.charAt(number);
            randomString.append(character);
        }
        return randomString.toString();
    }

    public int getRandomIntegerBetween(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public float getRandomFloatBetween(float min, float max) {
        return ThreadLocalRandom.current().nextFloat() * (max - min) + min;
    }

    public int getRandomGaussian(int mean, int standardDeviation) {
        double gaussian = ThreadLocalRandom.current().nextGaussian() * standardDeviation + mean;
        return (int) gaussian;
    }

}