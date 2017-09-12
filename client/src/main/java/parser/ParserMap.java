package parser;

import java.util.HashMap;
import java.util.Map;

public class ParserMap {

    public static Map<String, Class<? extends IParser>> parserMap;

    static {
        parserMap = new HashMap<>();
        parserMap.put("N", NewOrderParser.class);
    }

    public static Class<? extends IParser> get(String command) {
        return parserMap.get(command);
    }

}
