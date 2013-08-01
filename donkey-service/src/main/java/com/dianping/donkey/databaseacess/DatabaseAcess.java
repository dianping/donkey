package com.dianping.donkey.databaseacess;


/**
 * 
 * @author peng.hu
 *
 */

public interface DatabaseAcess {
	/**
	 * init the database connection
	 * @param url url of the database
	 * @param driver driver of the database
	 * @param userName username of the database
	 * @param passWord password of the database
	 */
	public void init(String url,String driver,String userName,String passWord) throws Exception;
	/**
	 * destroy the database connection
	 */
	public void destroy();
	/**
	 * 
	 * @param key query this key
	 * @return query result
	 */
	public String query(String key);
	/**
	 * get a id from the database
	 * @param key the name of the key you want to get
	 * @param num how many you want to put into memory cache
	 * @return allocated id
	 */
	public long getSomeID(String key,int num,String tablename) ;
}
