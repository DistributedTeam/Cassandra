package client.cs4224c.parser;

import java.util.HashMap;
import java.util.Map;

public class ParserMap {

    public static Map<String, Class<? extends AbstractParser>> parserMap;

    static {
        parserMap = new HashMap<>();
        parserMap.put("N", NewOrderParser.class);
        parserMap.put("P", PaymentParser.class);
        parserMap.put("D", DeliveryParser.class);
        parserMap.put("T", TopBalanceParser.class);
        parserMap.put("O", OrderStatusParser.class);
    }

    public static Class<? extends AbstractParser> get(String command) {
        return parserMap.get(command);
    }

}
