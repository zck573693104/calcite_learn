package org.apache.calcite.func;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateFormatFunc {
    /**
     * yyyy-MM-dd  yyyy-MM-dd HH:mm:ss
     * @param date
     * @param format
     * @return
     */
    public String eval(LocalDateTime date, String format){
        DateTimeFormatter fmDate = DateTimeFormatter.ofPattern(format);
        return date.format(fmDate);
    }
}
