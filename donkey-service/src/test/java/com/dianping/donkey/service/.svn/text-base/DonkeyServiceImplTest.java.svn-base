package com.dianping.donkey.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;

import com.dianping.donkey.databaseacess.DatabaseAcess;
import com.dianping.donkey.databaseacess.SqlDatabaseAcessImpl;
import com.dianping.donkey.memorytable.MemoryTableItem;
import com.dianping.donkey.memorytable.RouteTableItem;

public class DonkeyServiceImplTest {

	private DonkeyServiceImpl underTest;
	private Mockery context = new Mockery() {
		{
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private final DatabaseAcess sdai = (DatabaseAcess) context.mock(
			DatabaseAcess.class, "sdai");

	// @Test
	// public void testMyPrivateMethod() throws Exception {
	//
	// underTest = new DonkeyServiceImpl();
	//
	// Class[] parameterTypes = new Class[1];
	// parameterTypes[0] = java.lang.Integer.TYPE;
	//
	// Method m = underTest.getClass().getDeclaredMethod("setBatchSize",
	// parameterTypes);
	// m.setAccessible(true);
	//
	// Object[] parameters = new Object[1];
	// parameters[0] = 5569;
	//
	// int result = (Integer) m.invoke(underTest, parameters);
	//
	// // Do your assertions
	// assertNotNull(result);
	// }

	// get id when not in memory
	@Test
	public void testnotinmemory() throws Exception {
		underTest = new DonkeyServiceImpl();
		ConcurrentHashMap<String, RouteTableItem> rM = new ConcurrentHashMap<String, RouteTableItem>();
		ConcurrentHashMap<String, MemoryTableItem> mT = new ConcurrentHashMap<String, MemoryTableItem>();
		ConcurrentHashMap<String, ReentrantLock> mL = new ConcurrentHashMap<String, ReentrantLock>();
		ConcurrentHashMap<String, ReentrantLock> dL = new ConcurrentHashMap<String, ReentrantLock>();
		RouteTableItem rti = new RouteTableItem();
		rti.setDomain("donkey-service.group");
		rti.setLocation("10.1.77.17:3306");
		rti.setPassword(null);
		rti.setTablename("DP_Donkey");
		rti.setType("com.mysql.jdbc.Driver");
		rM.put(rti.getDomain(), rti);
		ConcurrentHashMap<String, DatabaseAcess> dbMap = new ConcurrentHashMap<String, DatabaseAcess>();
		final long value = 1000;
		context.checking(new Expectations() {
			{
				try {
					oneOf(sdai).getSomeID("donkey-service.group.test", 0,
							"DP_Donkey");
					will(returnValue(value));
				} catch (Exception e) {
					System.out.println("fail");
				}
			}
		});
		// System.out.println(sdai.getSomeID("test", 100, "DP_Donkey"));
		// context.assertIsSatisfied();
		dbMap.put("10.1.77.17:3306", sdai);
		Field privateStringField1 = DonkeyServiceImpl.class
				.getDeclaredField("routeMap");
		privateStringField1.setAccessible(true);
		privateStringField1.set(underTest, rM);
		Field privateStringField2 = DonkeyServiceImpl.class
				.getDeclaredField("dbMap");
		privateStringField2.setAccessible(true);
		privateStringField2.set(underTest, dbMap);
		Field privateStringField3 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMap");
		privateStringField3.setAccessible(true);
		privateStringField3.set(underTest, mT);
		Field privateStringField4 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMapLock");
		privateStringField4.setAccessible(true);
		privateStringField4.set(underTest, mL);
		Field privateStringField5 = DonkeyServiceImpl.class
				.getDeclaredField("dbMapLock");
		privateStringField5.setAccessible(true);
		privateStringField5.set(underTest, dL);
		Assert.assertEquals(1000, underTest.nextID("donkey-service.group",
				"test"));
		System.out.println(privateStringField2.get(underTest));
	}

	// get increment id
	@Test
	public void testincrementkey() throws Exception {
		underTest = new DonkeyServiceImpl();
		ConcurrentHashMap<String, RouteTableItem> rM = new ConcurrentHashMap<String, RouteTableItem>();
		ConcurrentHashMap<String, MemoryTableItem> mT = new ConcurrentHashMap<String, MemoryTableItem>();
		ConcurrentHashMap<String, ReentrantLock> mL = new ConcurrentHashMap<String, ReentrantLock>();
		ConcurrentHashMap<String, ReentrantLock> dL = new ConcurrentHashMap<String, ReentrantLock>();
		List<String> inc = new ArrayList<String>();
		inc.add("donkey-service.group.test");
		RouteTableItem rti = new RouteTableItem();
		rti.setDomain("donkey-service.group");
		rti.setLocation("10.1.77.17:3306");
		rti.setPassword(null);
		rti.setTablename("DP_Donkey");
		rti.setType("com.mysql.jdbc.Driver");
		rM.put(rti.getDomain(), rti);
		ConcurrentHashMap<String, DatabaseAcess> dbMap = new ConcurrentHashMap<String, DatabaseAcess>();
		final long value = 1000;
		context.checking(new Expectations() {
			{
				try {
					oneOf(sdai).getSomeID("donkey-service.group.test", 1,
							"DP_Donkey");
					will(returnValue(value));
				} catch (Exception e) {
					System.out.println("fail");
				}
			}
		});
		// System.out.println(sdai.getSomeID("test", 100, "DP_Donkey"));
		// context.assertIsSatisfied();
		dbMap.put("10.1.77.17:3306", sdai);
		Field privateStringField1 = DonkeyServiceImpl.class
				.getDeclaredField("routeMap");
		privateStringField1.setAccessible(true);
		privateStringField1.set(underTest, rM);
		Field privateStringField2 = DonkeyServiceImpl.class
				.getDeclaredField("dbMap");
		privateStringField2.setAccessible(true);
		privateStringField2.set(underTest, dbMap);
		Field privateStringField3 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMap");
		privateStringField3.setAccessible(true);
		privateStringField3.set(underTest, mT);
		Field privateStringField4 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMapLock");
		privateStringField4.setAccessible(true);
		privateStringField4.set(underTest, mL);
		Field privateStringField5 = DonkeyServiceImpl.class
				.getDeclaredField("dbMapLock");
		privateStringField5.setAccessible(true);
		privateStringField5.set(underTest, dL);
		Field privateStringField6 = DonkeyServiceImpl.class
				.getDeclaredField("increment");
		privateStringField6.setAccessible(true);
		privateStringField6.set(underTest, inc);
		long nextID = underTest.nextID("donkey-service.group", "test");
		Assert.assertEquals(1000, nextID);
		System.out.println(privateStringField2.get(underTest));
	}

	// get id when in memory
	@Test
	public void testinmemory() throws Exception {
		underTest = new DonkeyServiceImpl();
		ConcurrentHashMap<String, RouteTableItem> rM = new ConcurrentHashMap<String, RouteTableItem>();
		ConcurrentHashMap<String, MemoryTableItem> mT = new ConcurrentHashMap<String, MemoryTableItem>();
		ConcurrentHashMap<String, ReentrantLock> mL = new ConcurrentHashMap<String, ReentrantLock>();
		ConcurrentHashMap<String, ReentrantLock> dL = new ConcurrentHashMap<String, ReentrantLock>();
		MemoryTableItem mti = new MemoryTableItem();
		mti.setNextId(1000);
		mti.setLastNum(1500);
		mT.put("donkey-service.group.test", mti);
		RouteTableItem rti = new RouteTableItem();
		rti.setDomain("donkey-service.group");
		rti.setLocation("10.1.77.17:3306");
		rti.setPassword(null);
		rti.setTablename("DP_Donkey");
		rti.setType("com.mysql.jdbc.Driver");
		rM.put(rti.getDomain(), rti);
		ConcurrentHashMap<String, DatabaseAcess> dbMap = new ConcurrentHashMap<String, DatabaseAcess>();
		final long value = 1000;
		context.checking(new Expectations() {
			{
				try {
					oneOf(sdai).getSomeID("donkey-service.group.test", 0,
							"DP_Donkey");
					will(returnValue(value));
				} catch (Exception e) {
					System.out.println("fail");
				}
			}
		});
		// System.out.println(sdai.getSomeID("test", 100, "DP_Donkey"));
		// context.assertIsSatisfied();
		dbMap.put("10.1.77.17:3306", sdai);
		Field privateStringField1 = DonkeyServiceImpl.class
				.getDeclaredField("routeMap");
		privateStringField1.setAccessible(true);
		privateStringField1.set(underTest, rM);
		Field privateStringField2 = DonkeyServiceImpl.class
				.getDeclaredField("dbMap");
		privateStringField2.setAccessible(true);
		privateStringField2.set(underTest, dbMap);
		Field privateStringField3 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMap");
		privateStringField3.setAccessible(true);
		privateStringField3.set(underTest, mT);
		Field privateStringField4 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMapLock");
		privateStringField4.setAccessible(true);
		privateStringField4.set(underTest, mL);
		Field privateStringField5 = DonkeyServiceImpl.class
				.getDeclaredField("dbMapLock");
		privateStringField5.setAccessible(true);
		privateStringField5.set(underTest, dL);
		Assert.assertEquals(1000, underTest.nextID("donkey-service.group",
				"test"));
		System.out.println(privateStringField2.get(underTest));
	}

	// test id in memory but not enough
	@Test
	public void testinmemoryandstartthread() throws Exception {
		underTest = new DonkeyServiceImpl();
		ConcurrentHashMap<String, RouteTableItem> rM = new ConcurrentHashMap<String, RouteTableItem>();
		ConcurrentHashMap<String, MemoryTableItem> mT = new ConcurrentHashMap<String, MemoryTableItem>();
		ConcurrentHashMap<String, ReentrantLock> mL = new ConcurrentHashMap<String, ReentrantLock>();
		ConcurrentHashMap<String, ReentrantLock> dL = new ConcurrentHashMap<String, ReentrantLock>();
		MemoryTableItem mti = new MemoryTableItem();
		mti.setNextId(1000);
		mti.setLastNum(1000);
		mT.put("donkey-service.group.test", mti);
		RouteTableItem rti = new RouteTableItem();
		rti.setDomain("donkey-service.group");
		rti.setLocation("10.1.77.17:3306");
		rti.setPassword(null);
		rti.setTablename("DP_Donkey");
		rti.setType("com.mysql.jdbc.Driver");
		rM.put(rti.getDomain(), rti);
		ConcurrentHashMap<String, DatabaseAcess> dbMap = new ConcurrentHashMap<String, DatabaseAcess>();
		final long value = 2000;
		context.checking(new Expectations() {
			{
				try {
					oneOf(sdai).getSomeID("donkey-service.group.test", 1000,
							"DP_Donkey");
					will(returnValue(value));
				} catch (Exception e) {
					System.out.println("fail");
				}
			}
		});
		// System.out.println(sdai.getSomeID("test", 100, "DP_Donkey"));
		// context.assertIsSatisfied();
		dbMap.put("10.1.77.17:3306", sdai);
		Field privateStringField1 = DonkeyServiceImpl.class
				.getDeclaredField("routeMap");
		privateStringField1.setAccessible(true);
		privateStringField1.set(underTest, rM);
		Field privateStringField2 = DonkeyServiceImpl.class
				.getDeclaredField("dbMap");
		privateStringField2.setAccessible(true);
		privateStringField2.set(underTest, dbMap);
		Field privateStringField3 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMap");
		privateStringField3.setAccessible(true);
		privateStringField3.set(underTest, mT);
		Field privateStringField4 = DonkeyServiceImpl.class
				.getDeclaredField("memoryMapLock");
		privateStringField4.setAccessible(true);
		privateStringField4.set(underTest, mL);
		Field privateStringField5 = DonkeyServiceImpl.class
				.getDeclaredField("dbMapLock");
		privateStringField5.setAccessible(true);
		privateStringField5.set(underTest, dL);
		Field privateStringField6 = DonkeyServiceImpl.class
				.getDeclaredField("batchSize");
		privateStringField6.setAccessible(true);
		privateStringField6.setInt(underTest, 1000);
		Assert.assertEquals(2000, underTest.nextID("donkey-service.group",
				"test"));
		System.out.println(privateStringField2.get(underTest));
	}

	// test init when start service
	@Test
	public void testinit() throws Exception {
		underTest = new DonkeyServiceImpl();
		underTest.init();
		Assert.assertNotNull(underTest);
	}
}