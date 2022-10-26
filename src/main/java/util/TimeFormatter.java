package util;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

public class TimeFormatter {
    private static final Format dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static String getCurrentTimestamp() {
        return dateFormatter.format(new Date());
    }

    public static String formatTimestamp(LocalDate dateTime) {
        return dateFormatter.format(dateTime);
    }
}
