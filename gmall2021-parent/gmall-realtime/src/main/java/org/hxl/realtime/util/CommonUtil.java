package org.hxl.realtime.util;

/**
 * @author Grant
 * @create 2021-06-28 5:23
 */
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class CommonUtil {

    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<T>();

        it.forEach(list::add);

        return list;
    }

    public static Long toTs(String dateTime) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
