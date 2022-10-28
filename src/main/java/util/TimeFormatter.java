package util;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class TimeFormatter {
    private static final Format dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static Date getCurrentTimestamp() {
        return new Date();
    }

    public static String formatTimestamp(Date dateTime) {
        return dateFormatter.format(dateTime);
    }

    public static String formatTimestamp(Instant dateTime) {
        return dateFormatter.format(Date.from(dateTime));
    }
}
