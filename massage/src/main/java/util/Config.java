package util;

import java.nio.file.Paths;

public class Config {

    public static String getProjectRoot() {
        if (System.getProperty("user.dir") != null) {
            return Paths.get(System.getProperty("user.dir")).toFile().getParent();
        }
        return "";
    }
}
