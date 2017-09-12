package converter;

public abstract class AbstractConverter implements Runnable {
    public abstract void massage() throws Exception;

    @Override
    public void run() {
        try {
            this.massage();
        } catch (Exception e) {
            System.err.println(String.format("[%s] Massage task ends with exception %s", this.getClass().getSimpleName(), e.toString()));
        }
    }
}
