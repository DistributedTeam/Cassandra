package util;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

public class TimeUtility {

    private static final FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    public static Long parse(String datetime) {
        try {
            return fastDateFormat.parse(datetime).getTime();
        } catch (ParseException e) {
            return 0L;
        }
    }
}
