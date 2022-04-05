import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;

public class Util {
    // https://javadeveloperzone.com/java-basic/java-convert-long-to-byte-array/
    // Converts a java date to an 8 byte array in BigEndian format.
    static public byte[] dateToBytes(Date date) {
        long data;
        if (date != null) {
            data = date.getTime();
        } else {
            data = Long.MIN_VALUE;
        }
        return new byte[] {
                (byte) ((data >> 56) & 0xff),
                (byte) ((data >> 48) & 0xff),
                (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff),
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff),
        };
    }

    // Converts a java int to a 4 byte array in BigEndian format.
    static public byte[] intToBytes(int data) {
        return new byte[] {
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff),
        };
    }

    static public int bytesToInt(byte[] bytes) {
        return new BigInteger(bytes).intValue();
    }

    static public int[] bytesToIntArray(byte[] bytes) {
        int[] output = new int[bytes.length / 4];
        for (int i = 0; i + 4 <= bytes.length; i += 4) {
            output[i / 4] = bytesToInt(Arrays.copyOfRange(bytes, i, i + 4));
        }
        return output;
    }

    // Merges length bytes from source into destination, starting at sourceStart and
    // destinationStart
    static public void arrayMerge(byte[] source, byte[] destination, int length, int sourceStart,
            int destinationStart) {
        for (int i = 0; i < length; i++) {
            destination[i + destinationStart] = source[i + sourceStart];
        }
    }
}
