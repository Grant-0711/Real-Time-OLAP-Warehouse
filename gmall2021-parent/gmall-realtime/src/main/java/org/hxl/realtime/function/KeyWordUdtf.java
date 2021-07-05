package org.hxl.realtime.function;

/**
 * @author Grant
 * @create 2021-07-05 18:32
 */
import org.hxl.realtime.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Collection;

@FunctionHint(output = @DataTypeHint("row<word string>"))
public class KeyWordUdtf extends TableFunction<Row> {
    public void eval(String kw){
        Collection<String> kws = IkUtil.analyzer(kw);
        for (String w : kws) {
            collect(Row.of(w));
        }
    }
}
