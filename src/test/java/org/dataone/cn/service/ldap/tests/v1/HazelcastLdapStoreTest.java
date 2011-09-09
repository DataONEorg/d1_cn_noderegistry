/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.service.ldap.tests.v1;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import javax.annotation.Resource;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.ldap.HazelcastLdapStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.Set;
import org.dataone.service.types.v1.Node;
/**
 *
 * @author waltz
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:org/dataone/cn/service/ldap/tests/config/applicationContext.xml"})
public class HazelcastLdapStoreTest {
static Logger logger = Logger.getLogger(HazelcastLdapStoreTest.class);

    HazelcastLdapStore hazelcastLdapStore;
    private HazelcastInstance hazelcastInstance;
    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
  /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
    @Resource
    public void setHazelcastLdapStore(HazelcastLdapStore hazelcastLdapStore) {
        this.hazelcastLdapStore = hazelcastLdapStore;
    }
    @Resource
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
    @Test
    public void loadAllSimpleNodeTest() {
        logger.info("begin");
         Set<String> keys = hazelcastLdapStore.loadAllKeys();
         for (String key : keys) {
             logger.info("KEY: "+ key);
         }
         IMap<String, Node> d1NodesMap = hazelcastInstance.getMap("d1NodesMap");
         logger.info("Node map has " + d1NodesMap.size() + " entries");
         for (String key: d1NodesMap.keySet()) {
             Node node = d1NodesMap.get(key);
             logger.info("found node with id: " +key + " and url " + node.getBaseURL());
         }
    }


}
