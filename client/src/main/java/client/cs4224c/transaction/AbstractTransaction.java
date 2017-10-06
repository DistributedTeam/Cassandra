package client.cs4224c.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(AbstractTransaction.class);

    public abstract void executeFlow();

    public void execute() {
        logger.info("Begin to execute {}", this.getClass().getSimpleName());

        try {
            this.executeFlow();
        } catch (Exception e) {
            logger.error("Transaction {} gets error, {}", this.getClass().getSimpleName(), e);
        }

        logger.info("End execution {}", this.getClass().getSimpleName());
    }
}
