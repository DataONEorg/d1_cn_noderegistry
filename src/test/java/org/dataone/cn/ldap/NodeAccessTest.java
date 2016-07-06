package org.dataone.cn.ldap;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.ModificationItem;

import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.exceptions.MarshallingException;
import org.junit.Test;

public class NodeAccessTest {


//	@Test
//	public void testBuildNodeLdapName() {
//		NodeAccess na = new NodeAccess();
//		NodeReference nr = new NodeReference();
//		nr.setValue("urn:node:ATESTNODE");
//		
//		try {
//			na.buildNodeLdapName(nr);
//		} catch (InvalidNameException e) {
//			fail("Should not throw an InvalidNameException");
//		}
//	}
//	
//	@Test
//	public void testBuildNodeDN() throws InvalidNameException {
//		NodeAccess na = new NodeAccess();
//		NodeReference nr = new NodeReference();
//		nr.setValue("urn:node:ATESTNODE");
//		LdapName name = na.buildNodeLdapName(nr);
//		String expectedDN = "cn=urn:node:ATESTNODE,dc=dataone,dc=org";
//		
//		assertEquals(expectedDN, na.buildNodeDN(name));
//	}
//	
	
	@Test
	public void testMapSingle_SameValue() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;
		
		HashMap<String,NamingEnumeration<?>> map = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		atts.put(aName, "foo");
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {

			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			map.put(attributeName, attributeValue);
		}
		
		List<ModificationItem> mods = na.calcModifications(aName, map, "foo" );
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("there should be no modifications", mods.isEmpty());
	}
	
	@Test
	public void testMapSingle_DifferentValue() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> map = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		atts.put(aName, "foo");
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {

			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			map.put(attributeName, attributeValue);
		}

		List<ModificationItem> mods = na.calcModifications(aName, map, "bar" );
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}

		assertTrue("there should be one modification", mods.size() == 1);
		assertTrue("there should be one modification that is a 'Replace'", mods.get(0).toString().contains("Replace attribute:"));
	}
	

	@Test
	public void testMapSingle_FirstEntry() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> map = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		atts.put(aName, "foo");
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {

			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			map.put(attributeName, attributeValue);
		}

		List<ModificationItem> mods = na.calcModifications(NodeAccess.NODE_NAME, map, "foo" );
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("there should be one modification.", mods.size() == 1);
		assertTrue("the mod should be an 'Addition'",mods.get(0).toString().contains("Add attribute:") );

		
	}
		
	@Test
	public void testMapSingle_newEmptyEntry() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> map = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		atts.put(aName, "foo");
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {

			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			map.put(attributeName, attributeValue);
		}
		List<ModificationItem> mods = na.calcModifications(aName, map,"");
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("empty String should result in one modification", mods.size() == 1);
		assertTrue("empty String should replace the old value", mods.get(0).toString().contains("Replace attribute:"));
	}
	
	
	@Test
	public void testMapSingle_nullNewValue() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> map = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		atts.put(aName, "foo");
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {

			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			map.put(attributeName, attributeValue);
		}
		List<ModificationItem> mods = na.calcModifications(aName, map, null);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("empty String should result in one modification", mods.size() == 1);
		assertTrue("empty String should replace the old value", mods.get(0).toString().contains("Replace attribute:"));
	}
	
	@Test
	public void testMapList_sameValues() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<String> newList = new ArrayList<String>();
		newList.add("fool");
		newList.add("barb");
		
		List<ModificationItem> mods = na.calcListModifications(aName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("same values should produce no ModItems", mods.isEmpty());
	}
	
	@Test
	public void testMapList_oneSameOneNewValue() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<String> newList = new ArrayList<String>();
		newList.add("fool");
		newList.add("bash");
		
		List<ModificationItem> mods = na.calcListModifications(aName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("same values should produce two ModItems", mods.size() == 2);
		assertTrue("One Mod should be a Remove", mods.get(0).toString().contains("Remove attribute:") ||
				mods.get(1).toString().contains("Remove attribute:"));
		assertTrue("One Mod should be an Add", mods.get(0).toString().contains("Add attribute:") ||
				mods.get(1).toString().contains("Add attribute:"));
		String mod0 = mods.get(0).toString();
		String mod1 = mods.get(1).toString();
		assertTrue("the value removed should be 'barb'", (mod0.contains("barb") && mod0.contains("Remove")) ||
				mod1.contains("barb") && mod1.contains("Remove"));
		assertTrue("the value added should be 'bash'", (mod0.contains("bash") && mod0.contains("Add")) ||
				mod1.contains("bash") && mod1.contains("Add"));
	}
	
	@Test
	public void testMapList_addTwoMoreValues() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<String> newList = new ArrayList<String>();
		newList.add("fool");
		newList.add("bash");
		newList.add("bizz");
		newList.add("barb");
		
		List<ModificationItem> mods = na.calcListModifications(aName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("same values should produce one ModItem", mods.size() == 1);
		assertTrue("The ModItem should be an Add", mods.get(0).toString().contains("Add attribute:"));
		assertTrue("The ModItem should be contain the two new values", mods.get(0).toString().contains("bizz") &&
				mods.get(0).toString().contains("bash"));
	}
	
	@Test
	public void testMapList_noExistingValues() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;
		String anotherName = NodeAccess.NODE_ID;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<String> newList = new ArrayList<String>();
		newList.add("squish");
		newList.add("squash");
		
		// note getting modList for a different attribute
		List<ModificationItem> mods = na.calcListModifications(anotherName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("same values should produce one ModItem", mods.size() == 1);
		assertTrue("The ModItem should be an Add", mods.get(0).toString().contains("Add attribute:"));
		assertTrue("The ModItem should be contain the two new values", mods.get(0).toString().contains("squish") &&
				mods.get(0).toString().contains("squash"));
	}
	
	@Test
	public void testMapList_noNewValues() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<String> newList = new ArrayList<String>();
		
		// note getting modList for a different attribute
		List<ModificationItem> mods = na.calcListModifications(aName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("empty newValue list should produce one ModItem", mods.size() == 1);
		assertTrue("The ModItem should be a Remove", mods.get(0).toString().contains("Remove attribute:"));
	}
	
	@Test
	public void testMapList_nullNewValues() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_DESCRIPTION;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("fool");
		ba.add("barb");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		
		// note getting modList for a different attribute
		List<ModificationItem> mods = na.calcListModifications(aName, existingMap, null);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("null newValue list should produce zero ModItems", mods.size() == 1);
		assertTrue("The ModItem should be a Remove", mods.get(0).toString().contains("Remove attribute:"));
	}
	
	
	@Test
	public void testMapSubjectList_SubjectCompare_differentCase() throws NamingException {
		NodeAccess na = new NodeAccess();
		String aName = NodeAccess.NODE_CONTACT_SUBJECT;

		HashMap<String,NamingEnumeration<?>> existingMap = new HashMap<String,NamingEnumeration<?>>();

		Attributes atts = new BasicAttributes(true);
		BasicAttribute ba = new BasicAttribute(aName);
		ba.add("cn=huey,dc=dataone,dc=org");
		ba.add("cn=dewey,dc=dataone,dc=org");
		atts.put(ba);
		NamingEnumeration<? extends Attribute> values = atts.getAll();
		while (values.hasMore()) {
			Attribute attribute = values.next();
			String attributeName = attribute.getID().toLowerCase();
			NamingEnumeration<?> attributeValue = attribute.getAll();
			existingMap.put(attributeName, attributeValue);
		}
		List<Subject> newList = new ArrayList<Subject>();
		Subject s1 = new Subject(); s1.setValue("CN=huey,DC=dataone,DC=org"); newList.add(s1);
		Subject s2 = new Subject(); s2.setValue("cn=dewey,dc=dataone,dc=org"); newList.add(s2);
//		Subject s3 = new Subject(); s3.setValue("CN=louie,DC=dataone,DC=org"); newList.add(s3);
		
		List<ModificationItem> mods = na.calcSubjectListModifications(aName, existingMap, newList);
		for (ModificationItem item : mods) {
			System.out.println(item.toString());
		}
		assertTrue("should produce no ModItems", mods.size() == 0);
//		assertTrue("The ModItem should be an Add", mods.get(0).toString().contains("Add attribute:"));
//		assertTrue("The ModItem should be contain the two new values", mods.get(0).toString().contains("louie"));
	}
	

	
    private Node buildTestNode(String resourcePath) throws IOException, InstantiationException, IllegalAccessException, MarshallingException {
        ByteArrayOutputStream mnNodeOutput = new ByteArrayOutputStream();
        InputStream is = this.getClass().getResourceAsStream(resourcePath);
        
        BufferedInputStream bInputStream = new BufferedInputStream(is);
        byte[] barray = new byte[16384];
        int nRead = 0;
        while ((nRead = bInputStream.read(barray, 0, 16384)) != -1) {
            mnNodeOutput.write(barray, 0, nRead);
        }
        bInputStream.close();
        ByteArrayInputStream bArrayInputStream = new ByteArrayInputStream(mnNodeOutput.toByteArray());
        Node testNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);
        return testNode;
    }
}
