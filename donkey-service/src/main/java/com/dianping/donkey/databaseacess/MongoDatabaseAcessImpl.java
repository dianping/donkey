package com.dianping.donkey.databaseacess;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.dianping.donkey.DonkeyService;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


/**
 * 
 * @author peng.hu
 *
 */
public class MongoDatabaseAcessImpl implements DatabaseAcess {

	private static final Logger log = Logger
			.getLogger(MongoDatabaseAcessImpl.class);

	private Datastore ds;

	private String url;
	private String dbname;
	private Long number;
	private String driver;

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getDbname() {
		return dbname;
	}

	public void setNumber(Long number) {
		this.number = number;
	}

	public Long getNumber() {
		return number;
	}
	
	/**
	 * init the database connection
	 * @param url url of the database
	 * @param driver driver of the database
	 * @param userName username of the database
	 * @param passWord password of the database
	 * @throws Exception 
	 */
	public void init(String url, String driver, String userName, String passWord) throws Exception {
		Mongo m = null;
		try {
			m = new Mongo(url);
		} catch (UnknownHostException e) {
			log.error("Init Mongo error!", e);
			throw e;

		}
		DB db = m.getDB("donkey");
		Morphia morphia = new Morphia();
		morphia.map(DPDonkey.class);
		ds = morphia.createDatastore(m, db.getName());
	}

	/**
	 * destroy the database connection
	 */
	public void destroy() {
		return;
	}

	/**
	 * 
	 * @param key query this key
	 * @return query result
	 */
	public String query(String key) {
//		// clean unused code
//		try {
//			DBObject query = new BasicDBObject("name", key);
//			DBObject rs = ds.getCollection(DPDonkey.class).findOne(query);
//			if (rs == null) {
//				return null;
//			}
//			return rs.get("seq").toString();
//		} catch (MongoException e) {
//			// log.error("", e);
//			log.error("connect mongoDB fail when query " + key + ".", e);
//			return null;
//		}
		return null;
	}

	/**
	 * get a id from the database
	 * @param key the name of the key you want to get
	 * @param num how many you want to put into memory cache
	 * @return allocated id
	 */
	public long getSomeID(String key, int num, String tablename) {
		try {
			DBObject query = new BasicDBObject("name", key);
			DBObject update = new BasicDBObject("$inc", new BasicDBObject(
					"seq", num));
			DBObject rs = ds.getCollection(DPDonkey.class).findAndModify(query,
					null, null, false, update, false, true);
			Object seq = rs.get("seq");
			return seq == null ? 0 : Long.valueOf(seq.toString());
		} catch (MongoException e) {
			log.error("connect mongoDB fail when get " + key + "`s ID. ", e);
			return DonkeyService.Status.GETFAIL.code();
		}

	}
}
