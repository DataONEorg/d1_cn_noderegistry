/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.service.ldap.tests.v1;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.InputStream;
import java.util.Date;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.junit.Test;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.NodeList;
import org.junit.Before;
import static org.junit.Assert.*;
/**
 *
 * @author waltz
 */

public class HazelcastLdapStoreTest {
static Logger logger = Logger.getLogger(HazelcastLdapStoreTest.class);

    private HazelcastInstance hazelcastInstance;
    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
  /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
    @Before
    public void before() throws Exception {
        if (hazelcastInstance == null) {
         InputStream is = this.getClass().getResourceAsStream("/org/dataone/cn/service/ldap/tests/config/hazelcast.xml");

         XmlConfigBuilder configBuilder = new XmlConfigBuilder(is) ;

         hazelcastInstance = Hazelcast.newHazelcastInstance(configBuilder.build());
        }

    }
    @Test
    public void loadAllSimpleNodeTest() {
        try {
            Date now = new Date();
            IMap<String, Node> d1NodesMap = hazelcastInstance.getMap("hzNodes");
            logger.info("Node map has " + d1NodesMap.size() + " entries");
            for (String key : d1NodesMap.keySet()) {
                Node node = d1NodesMap.get(key);
                logger.info("found node with id: " + key + " and url " + node.getBaseURL());
                if (node.getType().equals(NodeType.MN)) {
                node.getSynchronization().setLastHarvested(now);
                }
                d1NodesMap.put(key, node);
            }
            NodeRegistryService nodeRegistryService = new NodeRegistryService();
            NodeList nodeList = nodeRegistryService.listNodes();
            for (Node node : nodeList.getNodeList()) {
                 if ((node.getType().equals(NodeType.MN)) &&
                         !(node.getSynchronization().getLastHarvested().equals(now))) {
                      fail("setLastHarvested did not update correctly!");
                 }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("loadAllSimpleNodeTest");
        }
    }


}
