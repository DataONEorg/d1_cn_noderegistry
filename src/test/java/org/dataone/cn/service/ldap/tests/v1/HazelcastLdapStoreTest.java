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
import org.apache.log4j.Logger;
import org.junit.Test;
import org.dataone.service.types.v1.Node;
import org.junit.Before;
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

         IMap<String, Node> d1NodesMap = hazelcastInstance.getMap("hzNodes");
         logger.info("Node map has " + d1NodesMap.size() + " entries");
         for (String key: d1NodesMap.keySet()) {
             Node node = d1NodesMap.get(key);
             logger.info("found node with id: " +key + " and url " + node.getBaseURL());
         }
    }


}
