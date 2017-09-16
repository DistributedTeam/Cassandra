package parser;

import transaction.ITransaction;

import java.util.Scanner;

public interface IParser {

    ITransaction parse(Scanner sc, String[] arguments);

}
