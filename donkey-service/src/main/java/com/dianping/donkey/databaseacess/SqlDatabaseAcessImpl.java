package com.dianping.donkey.databaseacess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.dianping.donkey.DonkeyService;
import com.dianping.donkey.service.DonkeyServiceImpl;
import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * 
 * @author peng.hu
 * 
 */
public class SqlDatabaseAcessImpl implements DatabaseAcess {
	private static final Logger log = Logger.getLogger(DonkeyServiceImpl.class);
	private static final String SQL_QUERY = "SELECT * FROM %s WHERE name = ? FOR UPDATE";
	private static final String SQL_UPDATE = "UPDATE %s SET seq = ?, UpdateTime=NOW() WHERE name = ?";
	private static final String SQL_INSERT = "INSERT INTO %s (name,seq,AddTime,UpdateTime) VALUES(?,1,NOW(),NOW())";
	private DataSource ds;
	private String driver;

	public DataSource getDs() {
		return ds;
	}

	public void setDs(DataSource ds) {
		this.ds = ds;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	/**
	 * init the database connection
	 * 
	 * @param url
	 *            url of the database
	 * @param driver
	 *            driver of the database
	 * @param userName
	 *            username of the database
	 * @param passWord
	 *            password of the database
	 */
	public void init(String url, String driver, String userName, String passWord)
			throws Exception {
		try {
			ComboPooledDataSource c3p0DS = new ComboPooledDataSource();
			c3p0DS.setJdbcUrl(url);
			c3p0DS.setUser(userName);
			c3p0DS.setPassword(passWord);
			c3p0DS.setDriverClass(driver);
			c3p0DS.setMinPoolSize(2);
			c3p0DS.setMaxPoolSize(15);
			c3p0DS.setInitialPoolSize(5);
			c3p0DS.setMaxIdleTime(1800);
			c3p0DS.setIdleConnectionTestPeriod(60);
			c3p0DS.setAcquireRetryAttempts(3);
			c3p0DS.setAcquireRetryDelay(300);
			c3p0DS.setMaxStatements(0);
			c3p0DS.setMaxStatementsPerConnection(100);
			c3p0DS.setNumHelperThreads(6);
			c3p0DS.setMaxAdministrativeTaskTime(5);
			c3p0DS.setLoginTimeout(2); // abandoned the attempt after 60s
			c3p0DS.setPreferredTestQuery("SELECT 1");
			ds = c3p0DS;
		} catch (Exception e) {
			log.error("Init database connection pool error!", e);
			throw e;
		}
	}

	/**
	 * destroy the database connection
	 */
	public void destroy() {
		if (ds != null) {
			((ComboPooledDataSource) ds).close();
		}
	}

	/**
	 * 
	 * @param key
	 *            query this key
	 * @return query result
	 */
	public String query(String key) {
		// String sql = SQL_QUERY;
		// if (ds != null) {
		// Connection conn = null;
		// PreparedStatement pstmt = null;
		// try {
		// conn = (Connection) ds.getConnection();
		// pstmt = conn.prepareStatement(sql);
		// pstmt.setString(1, key);
		// ResultSet rs = pstmt.executeQuery();
		// if (rs.next()) {
		// return rs.getString("seq");
		// }
		// return null;
		// } catch (SQLException e) {
		// log.error("connect fail when query " + key + ".", e);
		// } finally {
		// if (pstmt != null) {
		// try {
		// pstmt.close();
		// } catch (Exception e) {
		// log.error("close pstmt fail.", e);
		// }
		// }
		// if (conn != null) {
		// try {
		// conn.close();
		// } catch (Exception e) {
		// log.error("close connection fail.", e);
		// }
		// }
		// }
		// }
		return null;
	}

	public int update(String key, long id) {
		// String sql = SQL_UPDATE;
		// if (ds != null) {
		// Connection conn = null;
		// PreparedStatement pstmt = null;
		// try {
		// conn = (Connection) ds.getConnection();
		// pstmt = conn.prepareStatement(sql);
		// pstmt.setString(1, String.valueOf(id));
		// pstmt.setString(2, key);
		// return pstmt.executeUpdate();
		// } catch (SQLException e) {
		// } finally {
		// if (pstmt != null) {
		// try {
		// pstmt.close();
		// } catch (Exception e) {
		// }
		// }
		// if (conn != null) {
		// try {
		// conn.close();
		// } catch (Exception e) {
		//
		// }
		// }
		// }
		// }
		return 0;
	}

	/**
	 * get a id from the database
	 * 
	 * @param key
	 *            the name of the key you want to get
	 * @param num
	 *            how many you want to put into memory cache
	 * @return allocated id
	 */
	public long getSomeID(String key, int num, String tablename) {
		if (ds != null) {
			Connection conn = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				conn = (Connection) ds.getConnection();
				conn.setAutoCommit(false);
				pstmt = conn.prepareStatement(String.format(SQL_QUERY,
						tablename));
				pstmt.setString(1, key);
				rs = pstmt.executeQuery();
				if (!rs.next()) {
					pstmt = conn.prepareStatement(String.format(SQL_INSERT,
							tablename));
					pstmt.setString(1, key);
					pstmt.executeUpdate();
					pstmt = conn.prepareStatement(String.format(SQL_QUERY,
							tablename));
					pstmt.setString(1, key);
					rs = pstmt.executeQuery();
					if (!rs.next()) {
						log.error("Create table`s item fail when get" + key
								+ "`s ID.");
						return DonkeyService.Status.GETFAIL.code();
					}
				}
				long id = Long.parseLong(rs.getString("seq")) + num;
				pstmt = conn.prepareStatement(String.format(SQL_UPDATE,
						tablename));
				// pstmt.setString(1, tablename);
				pstmt.setString(1, String.valueOf(id));
				pstmt.setString(2, key);
				pstmt.executeUpdate();
				conn.commit();
				conn.setAutoCommit(true);
				return id - num;
			} catch (SQLException e) {
				try {
					log.error("Operate database fail when get " + key
							+ "`s ID.", e);
					conn.rollback();
					conn.setAutoCommit(true);
					return DonkeyService.Status.GETFAIL.code();
				} catch (SQLException se1) {
					log.error("Rollback fail when get " + key + "`s ID.", e);
					return DonkeyService.Status.GETFAIL.code();
				}

			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						log.error("Close ResultSet fail when get " + key
								+ "`s ID. ", e);
					}
				}
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (Exception e) {
						log.error("Close pstmt fail when get " + key
								+ "`s ID. ", e);
					}
				}
				if (conn != null) {
					try {
						conn.close();
					} catch (Exception e) {
						log.error("Close connection fail when get " + key
								+ "`s ID. ", e);
					}
				}
			}
		}
		return DonkeyService.Status.GETFAIL.code();
	}

}
