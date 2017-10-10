package client.cs4224c.policy;

import client.cs4224c.util.Constant;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ExperimentLoadBalancePolicy implements LoadBalancingPolicy {

    private static final Logger logger = LoggerFactory.getLogger(ExperimentLoadBalancePolicy.class);

    private ConcurrentHashMap<String, Host> hostMap;

    private String hostName;

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        hostMap = new ConcurrentHashMap<>();
        hostName = System.getProperty(Constant.PROPERTY_EXPERIMENT_HOST);
        if (StringUtils.isEmpty(hostName)) {
            throw new RuntimeException("Need specify experiment host for load balance policy");
        }
        logger.info("ExperimentLoadBalancePolicy is in use. This is not the most efficient policy.");

        for (Host host : hosts) {
            onAdd(host);
        }
        logger.info("Host map is {}", hostMap.toString());
        logger.info("Requested host is {}", hostName);
    }

    @Override
    public HostDistance distance(Host host) {
        return HostDistance.LOCAL;
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        return new AbstractIterator<Host>() {
            @Override
            protected Host computeNext() {
                return hostMap.get(hostName);
            }
        };
    }

    @Override
    public void onUp(Host host) {
        logger.info("Add host {}", host.getAddress().getHostAddress());
        hostMap.putIfAbsent(host.getAddress().getHostAddress(), host);
    }

    @Override
    public void onDown(Host host) {
        logger.info("Remove host {}", host.getAddress().getHostAddress());
        hostMap.remove(host.getAddress().getHostAddress());
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
