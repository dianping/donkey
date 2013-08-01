package com.dianping.donkey.databaseacess;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.context.ApplicationContext;

public class SqlDatabaseAcessImplTest extends DonkeyMultiDBBaseTest {
	private ApplicationContext context2;

	@Override
	protected String getDBBaseUrl() {
		return "jdbc:h2:mem:";
	}

	@Override
	protected String getCreateScriptConfigFile() {
		return "db-datafiles/createtable.xml";
	}

	@Override
	protected String getDataFile() {
		return "db-datafiles/data.xml";
	}

	@Override
	protected String[] getSpringConfigLocations() {
		return new String[] { "databasefile.xml" };
	}

	@Test
	public void testgetSomeIDdsnotinit() throws Exception {
		// if (context2 == null) {
		// context2 = new
		// ClassPathXmlApplicationContext("databasefile.xml");
		// }
		SqlDatabaseAcessImpl s = new SqlDatabaseAcessImpl();
		Assert.assertEquals(-1, s.getSomeID("donkey.service.group.test", 1000,
				"DP_Donkey")); // ds未初始化
	}

	@Test
	public void testgetSomeIDnokey() throws Exception {
		DataSource ds = (DataSource) context.getBean("id0");
		SqlDatabaseAcessImpl s = new SqlDatabaseAcessImpl();
		s.setDs(ds);
		long a = s.getSomeID("donkey.service.group.hupeng", 1000, "DP_Donkey");
		Assert.assertEquals(1, a);// ds已初始化 数据库中没有donkey.service.group.hupeng
	}

	@Test
	public void testgetSomeIDhaskey() throws Exception {
		DataSource ds = (DataSource) context.getBean("id0");
		SqlDatabaseAcessImpl s = new SqlDatabaseAcessImpl();
		s.setDs(ds);
		Assert.assertEquals(1000, s.getSomeID("donkey.service.group.test",
				1000, "DP_Donkey"));// ds已初始化 数据库中已有donkey.service.group.test
	}

	@Test
	public void testinit() throws Exception {
		SqlDatabaseAcessImpl a = new SqlDatabaseAcessImpl();
		a.init("127.0.0.1:3306", "com.mysql.jdbc.Driver", "", "");
		Assert.assertNotNull(a);
	}
}
