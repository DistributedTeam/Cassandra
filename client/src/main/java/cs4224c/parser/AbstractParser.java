package cs4224c.parser;

import cs4224c.transaction.AbstractTransaction;

import java.util.Scanner;

public abstract class AbstractParser {

    public abstract AbstractTransaction parse(Scanner sc, String[] arguments);

}
