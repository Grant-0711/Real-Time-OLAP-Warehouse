package org.hxl.realtime.util;

/**
 * @author Grant
 * @create 2021-07-05 18:34
 */
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;

public class IkUtil {
    public static void main(String[] args) {
        System.out.println(analyzer("手机华为手机荣耀手机"));
    }

    public static Collection<String> analyzer(String text) {
        Collection<String> result = new HashSet<>();
        // 我是中国人  smart: 我 是 中国人   max_word: 我 是 中国人 中国 国人

        // 如何把text 字符串转成 Reader 字符输入流?

        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(text), true);
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
