package org.hxl.realtime.bean;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Grant
 * @create 2021-06-26 4:57
 */
public class CommonUtil {

    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<T>();

        it.forEach(list::add);

        return list;
    }
}
