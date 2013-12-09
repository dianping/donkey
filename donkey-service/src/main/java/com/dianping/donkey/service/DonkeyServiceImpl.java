package com.dianping.donkey.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.donkey.DonkeyService;
import com.dianping.donkey.databaseacess.DatabaseAcess;
import com.dianping.donkey.memorytable.DatabaseTable;
import com.dianping.donkey.memorytable.MemoryTableItem;
import com.dianping.donkey.memorytable.RouteTableItem;
import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

/**
 * donkey server`s interface
 * 
 * @author peng.hu
 * 
 */
public class DonkeyServiceImpl implements BeanFactoryAware, DonkeyService {
	private static final Logger log = Logger.getLogger(DonkeyServiceImpl.class);
	private static final String ROUTE_KEY = "donkey-service.route";
	private static final String INCREMENT_KEY = "donkey-service.increment";
	private static final String BATCHSIZE = "donkey-service.batchSize";
	private ConcurrentHashMap<String, MemoryTableItem> memoryMap;
	private ConcurrentHashMap<String, ReentrantLock> memoryMapLock;
	private ConfigCache routeConfig;
	private ConcurrentHashMap<String, RouteTableItem> routeMap;
	private ConcurrentHashMap<String, DatabaseAcess> dbMap;
	private ConcurrentHashMap<String, ReentrantLock> dbMapLock;
	private volatile List<String> increment;
	private volatile List<String> domainList;
	private int batchSize;
	private BeanFactory beanFactory;

	public void setDomainList(List<String> domainList) {
		this.domainList = domainList;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		if (batchSize <= 0) {
			this.batchSize = 1;
			return;
		}
		this.batchSize = batchSize;
	}

	public void setIncrement(List<String> increment) {
		this.increment = increment;
	}

	private void createConnection(String domain) throws Exception {
		DatabaseAcess tmp = (DatabaseAcess) beanFactory.getBean(routeMap.get(
				domain).getType());
		tmp.init(routeMap.get(domain).getLocation(), routeMap.get(domain)
				.getType(), routeMap.get(domain).getUsername(), routeMap.get(
				domain).getPassword());
		dbMap.put(routeMap.get(domain).getLocation(), tmp);
	}

	/**
	 * init donkey server
	 * 
	 * @throws LionException
	 */
	public void init() throws LionException {
		memoryMap = new ConcurrentHashMap<String, MemoryTableItem>();
		routeMap = new ConcurrentHashMap<String, RouteTableItem>();
		dbMap = new ConcurrentHashMap<String, DatabaseAcess>();
		memoryMapLock = new ConcurrentHashMap<String, ReentrantLock>();
		dbMapLock = new ConcurrentHashMap<String, ReentrantLock>();
		routeConfig = ConfigCache
				.getInstance(EnvZooKeeperConfig.getZKAddress());
		this.batchSize = Integer.parseInt(routeConfig.getProperty(BATCHSIZE));
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.equals(BATCHSIZE)) {
					setBatchSize(Integer.parseInt(value));
					log.warn("batchsize has changed to " + value + ".");
				}
			}
		});
		final String increment = routeConfig.getProperty(INCREMENT_KEY);
		Gson gson = new Gson();
		try {
			this.increment = gson.fromJson(increment,
					new TypeToken<List<String>>() {
					}.getType());
		} catch (JsonParseException e) {
			log.warn("Json parse error when read increment from zookeeper ", e);
			throw e;
		}

		routeConfig.addChange(new ConfigChange() {
			// register call back function
			@SuppressWarnings("unchecked")
			public void onChange(String key, String value) {
				if (key != null && key.equals(INCREMENT_KEY)) {
					Gson gson = new Gson();
					try {
						setIncrement((List<String>) gson.fromJson(value,
								new TypeToken<List<String>>() {
								}.getType()));
					} catch (JsonParseException e) {
						log.warn("Json parse error when read increment from zookeeper ",e);
						throw e;
					}
					log.warn("increment key has change to " + value + ".");
				}
			}
		});

		String value = routeConfig.getProperty(ROUTE_KEY);
		try {
			this.domainList = gson.fromJson(value,
					new TypeToken<ArrayList<String>>() {
					}.getType());
		} catch (JsonParseException e) {
			log.warn("Json parse error when read route from zookeeper ", e);
			throw e;
		}
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			@SuppressWarnings("unchecked")
			public void onChange(String key, String value) {
				if (key != null && key.equals(ROUTE_KEY)) {
					Gson gson = new Gson();
					try {
						setDomainList((List<String>) gson.fromJson(value,
								new TypeToken<List<String>>() {
								}.getType()));
					} catch (JsonParseException e) {
						log
								.warn(
										"Json parse error when read route from zookeeper ",
										e);
						throw e;
					}
					log.warn("route key has change to " + value + ".");
					routeMap.clear();
					int len = domainList.size();
					int i = 0;
					String v = null;
					while (i < len) {
						try {
							v = routeConfig.getProperty(domainList.get(i));
						} catch (LionException e) {
							log.error("connect zookeeper error! when get "
									+ domainList.get(i) + ".", e);
						}
						if (v == null) {
							log.error("A key has set to null!");
							i++;
							continue;
						}

						try {
							addRouteItem(v, domainList.get(i));
						} catch (LionException e) {
							log.error("connect zookeeper error! when get "
									+ domainList.get(i) + ".", e);
						}
						i++;
					}
				}
			}
		});
		int len = domainList.size();
		int i = 0;
		while (i < len) {
			value = routeConfig.getProperty(domainList.get(i));
			if (value == null) {
				i++;
				continue;
			}
			addRouteItem(value, domainList.get(i));
			addChange(domainList.get(i));
			i++;
		}

	}

	/**
	 * register onchange function when a domain change
	 * 
	 * @param domain
	 *            name of the domain
	 */
	public void addChange(final String domain) {
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.startsWith("donkey-service")
						&& !key.equals("donkey-service.route")
						&& !key.equals("donkey-service.increment")
						&& !key.equals("donkey-service.batchSize")
						&& !key.equals("donkey-service.heartbeatInterval")
						) {
					// memoryMap.put(domain, value);
					try {
						addRouteItem(value, domain);
					} catch (LionException e) {
						log.error("connect zookeeper error! when get " + key
								+ ".", e);
					}
				}
			}
		});
	}

	/**
	 * register onchange function when url change
	 * 
	 * @param location
	 */
	public void addChangeUrl(final String location) {
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.equals(location)) {
					try {
						for (Iterator<String> iter = routeMap.keySet()
								.iterator(); iter.hasNext();) {
							String k = iter.next();
							if (routeMap.get(k).getLocationTag() == location) {
								String oldurl = routeMap.get(k).getLocation();
								String v = routeConfig.getProperty(location);
								log.warn("Domain "
										+ routeMap.get(k).getDomain()
										+ "has change url " + oldurl + " to "
										+ v);
								routeMap.get(k).setLocation(v);
								if (v.contains("mysql")) {
									routeMap.get(k).setType(
											"com.mysql.jdbc.Driver");
								} else{
									routeMap.get(k).setType("mongo");
								}
							}
						}
					} catch (LionException e) {
						log.error("connect zookeeper error! when get " + key
								+ ".", e);
					}
				}
			}
		});
	}

	/**
	 * register onchange function when username change
	 * 
	 * @param username
	 */
	public void addChangeUsername(final String username) {
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.equals(username)) {
					try {
						for (Iterator<String> iter = routeMap.keySet()
								.iterator(); iter.hasNext();) {
							String k = iter.next();
							if (routeMap.get(k).getUsernameTag() == username) {
								String oldusername = routeMap.get(k)
										.getUsername();
								routeMap.get(k).setUsername(
										(routeConfig.getProperty(username)));
								log.warn("Domain "
										+ routeMap.get(k).getDomain()
										+ " location "
										+ routeMap.get(k).getLocation()
										+ "has change username " + oldusername
										+ " to "
										+ routeMap.get(k).getUsername());
								dbMap.get(routeMap.get(k).getLocation())
										.destroy();
								dbMap.remove(routeMap.get(k).getLocation());
							}
						}
					} catch (LionException e) {
						log.error("connect zookeeper error! when get " + key
								+ ".", e);

					}
				}
			}
		});
	}

	/**
	 * register onchange function when password change
	 * 
	 * @param password
	 */
	public void addChangePassword(final String password) {
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.equals(password)) {
					try {
						for (Iterator<String> iter = routeMap.keySet()
								.iterator(); iter.hasNext();) {
							String k = iter.next();
							if (routeMap.get(k).getPasswordTag() == password) {
								String oldpsw = routeMap.get(k).getPassword();
								routeMap.get(k).setPassword(
										(routeConfig.getProperty(password)));
								log.warn("Domain "
										+ routeMap.get(k).getDomain()
										+ " location "
										+ routeMap.get(k).getLocation()
										+ "has change password " + oldpsw
										+ " to "
										+ routeMap.get(k).getPassword());
								dbMap.get(routeMap.get(k).getLocation())
										.destroy();
								dbMap.remove(routeMap.get(k).getLocation());
							}

						}
					} catch (LionException e) {
						log.error("connect zookeeper error! when get " + key
								+ ".", e);
					}
				}
			}
		});
	}
	
	/**
	 * register onchange function when tablename change
	 * 
	 * @param tablename
	 */
	public void addChangeTableName(final String tablename) {
		routeConfig.addChange(new ConfigChange() {
			// register call back function
			public void onChange(String key, String value) {
				if (key != null && key.equals(tablename)) {
					try {
						for (Iterator<String> iter = routeMap.keySet()
								.iterator(); iter.hasNext();) {
							String k = iter.next();
							if (routeMap.get(k).getTablenameTag() == tablename) {
								String oldtn = routeMap.get(k).getTablename();
								routeMap.get(k).setTablename(
										(routeConfig.getProperty(tablename)));
								log.warn("Domain "
										+ routeMap.get(k).getDomain()
										+ " location "
										+ routeMap.get(k).getLocation()
										+ "has change tablename " + oldtn
										+ " to "
										+ routeMap.get(k).getTablename());
							}

						}
					} catch (LionException e) {
						log.error("connect zookeeper error! when get " + key
								+ ".", e);
					}
				}
			}
		});
	}

	public void lockmemoryMap(String name) {
		memoryMapLock.putIfAbsent(name, new ReentrantLock());
		memoryMapLock.get(name).lock();
	}

	public void unlockmemoryMap(String name) {
		memoryMapLock.get(name).unlock();
	}

	public void lockdbMap(String name) {
		dbMapLock.putIfAbsent(name, new ReentrantLock());
		dbMapLock.get(name).lock();
	}

	public void unlockdbMap(String name) {
		dbMapLock.get(name).unlock();
	}

	/**
	 * 
	 * @param domain
	 * @param name
	 * @return 0 success, -1 fail
	 * @throws Exeption
	 */
	public int checkConnection(String domain) throws Exception {
		lockdbMap(domain);
		try {
			if (routeMap.get(domain) != null) {
				if (!dbMap.containsKey(routeMap.get(domain).getLocation())){
					createConnection(domain);
				}
				return 0;
			} else {
				log.error("routeMap Didn`t contain this domain");
				return DonkeyService.Status.INVALIDKEY.code();
			}
		} finally {
			unlockdbMap(domain);
		}
	}

	/**
	 * allocate next id
	 * 
	 * @param domain
	 *            name of the domain
	 * @param key
	 *            name of the key
	 * @return allocated Id
	 */
	public long nextID(final String domain, String key) {
		long begintime = System.currentTimeMillis();
		long result = 0;
		Transaction trans = Cat.getProducer().newTransaction("nextID", "nextID");
		if (domain == null || key == null) {
			trans.setStatus("domain or key NULL");
			trans.complete();
			return DonkeyService.Status.INVALIDKEY.code();
		}
		long tmpid;
		final String name = domain + "." + key;
		if (increment != null) {
			if (increment.contains(name)) {
				try {
					if (checkConnection(domain) == DonkeyService.Status.INVALIDKEY
							.code()) {
						trans.setStatus("invalid key");
						trans.complete();
						return DonkeyService.Status.INVALIDKEY.code();
					}
				} catch (Exception e) {
					trans.setStatus("Connect database error!");
					trans.complete();
					log.error("Connect database error!");
					return DonkeyService.Status.INVALIDKEY.code();
				}
				result = dbMap.get(routeMap.get(domain).getLocation())
						.getSomeID(name, 1, routeMap.get(domain).getTablename());
				if (result == DonkeyService.Status.INVALIDKEY.code()) {
					log.info("Get " + name + " nextid fail, use time "
							+ (System.currentTimeMillis() - begintime));
				} else {
					log.info("Get " + name + " nextid success, use time "
							+ (System.currentTimeMillis() - begintime));
				}
				trans.setStatus(Message.SUCCESS);
				trans.complete();
				return result;
			}
		}
		// in the memorytable

		try {
			lockmemoryMap(name);
			if (memoryMap.containsKey(name)) {
				while (true) {
					tmpid = memoryMap.get(name).getNextId();
					log.info("the tmpid is " + tmpid);
					if (tmpid >= memoryMap.get(name).getLastNum() - batchSize
							/ 10
							&& !memoryMap.get(name).getAsThreadStart()
							&& memoryMap.get(name).isPosHasChange()) {
						log.info("Prefetch... Id: " + tmpid + " lastNum: "
								+ memoryMap.get(name).getLastNum());
						memoryMap.get(name).setAsThreadStart(true);
						Thread t = new Thread(new Runnable() {
							public void run() {
								try {
									try {
										checkConnection(domain);
									} catch (Exception e) {
										log
												.error("Asynchronous Thread Connect database error!");
									}
									long tmpid = dbMap.get(
											routeMap.get(domain).getLocation())
											.getSomeID(name, batchSize, routeMap.get(domain).getTablename());
									if (tmpid == DonkeyService.Status.GETFAIL
											.code()) {
										log.info("Asynchronous Thread Get "
												+ name
												+ " nextid fail, use time ");
									} else {
										memoryMap.get(name).setOtherNextId(
												tmpid);
										memoryMap.get(name).setOtherLastNum(
												tmpid + batchSize);
										memoryMap.get(name).setPosHasChange(
												false);
										log.info("Async set lastnum to "
												+ (tmpid + batchSize));
									}
								} finally {
									memoryMap.get(name).setAsThreadStart(false);
									// synchronized (memoryMap.get(name)) {
									// memoryMap.get(name).notifyAll();
									// }
								}
							}
						});
						t.start();
					}
					if (tmpid < memoryMap.get(name).getLastNum()) {
						log.info("Get " + name + " nextid success, use time "
								+ (System.currentTimeMillis() - begintime));
						trans.setStatus(Message.SUCCESS);
						return tmpid;
					} else {
						while (true) {
							if (!memoryMap.get(name).getAsThreadStart()) {
								if (!memoryMap.get(name).isPosHasChange()) {
									memoryMap.get(name).posChange();
									memoryMap.get(name).setPosHasChange(true);
								}
								break;
							}
						}
						// synchronized (memoryMap.get(name)) {
						// log.info("the tmpid " + tmpid
						// + " has enter the sync");
						// while (true) {
						// log.info("I have enter "
						// + memoryMap.get(name).getLastNum());
						// try {
						// memoryMap.get(name).wait();
						// if (!memoryMap.get(name).getAsThreadStart())
						// break;
						// } catch (InterruptedException e) {
						// // TODO Auto-generated catch block
						// e.printStackTrace();
						// }
						// }
						// // log.info("after change nextid = " +
						// // memoryMap.get(name).getNextId() + "lastnum = " +
						// // memoryMap.get(name).getLastNum());
						// if (!memoryMap.get(name).isPosHasChange()) {
						// memoryMap.get(name).posChange();
						// memoryMap.get(name).setPosHasChange(true);
						// }
						// }
					}
				}
				// } else {
				// if( AsThreadStart.get() == true )
				// return DonkeyService.Status.GETFAIL.code();
				// try {
				// if (checkConnection(domain) ==
				// DonkeyService.Status.INVALIDKEY
				// .code()) {
				// return DonkeyService.Status.INVALIDKEY.code();
				// }
				// } catch (Exception e) {
				// log.error("Connect database error!");
				// return DonkeyService.Status.INVALIDKEY.code();
				// }
				// tmpid = dbMap.get(routeMap.get(domain).getLocation())
				// .getSomeID(name, batchSize);
				// if (tmpid == DonkeyService.Status.GETFAIL.code()) {
				// log.info("Get " + name + " nextid fail, use time "
				// + (System.currentTimeMillis() - begintime));
				// return DonkeyService.Status.GETFAIL.code();
				// }
				// memoryMap.get(name).setLastNum(tmpid + batchSize);
				// log.info("Set lastnum to " + (tmpid + batchSize));
				// memoryMap.get(name).setNextId(tmpid + 1);
				// log.info("Get " + name + " nextid " + tmpid
				// + " success, use time "
				// + (System.currentTimeMillis() - begintime));
				// return tmpid;
				//
				// }

			}

			// not in the memorytable
			else {
				if (!routeMap.containsKey(domain)) {
					trans.setStatus("invalid key");
					return DonkeyService.Status.INVALIDKEY.code();
				}
				MemoryTableItem mItem = new MemoryTableItem();
				mItem.setIncrement(false);
				mItem.setLastNum(0);
				mItem.setLocation(routeMap.get(domain).getLocation());
				mItem.setName(name);
				mItem.setNextId(0);
				mItem.setType(routeMap.get(domain).getType());
				try {
					if (checkConnection(domain) == DonkeyService.Status.INVALIDKEY
							.code()) {
						trans.setStatus("invalid key");
						return DonkeyService.Status.INVALIDKEY.code();
					}
				} catch (Exception e) {
					log.error("Connect database error!");
					trans.setStatus("Connect database error!");
					return DonkeyService.Status.INVALIDKEY.code();
				}
				tmpid = dbMap.get(routeMap.get(domain).getLocation())
						.getSomeID(name, batchSize,routeMap.get(domain).getTablename());

				if (tmpid == DonkeyService.Status.GETFAIL.code()) {
					log.info("Get " + name + " nextid fail, use time "
							+ (System.currentTimeMillis() - begintime));
					trans.setStatus("get nextID fail!");
					return DonkeyService.Status.GETFAIL.code();
				}
				mItem.setLastNum(tmpid + batchSize);
				log.info("Set lastnum to " + (tmpid + batchSize));
				mItem.setNextId(tmpid + 1);
				memoryMap.put(name, mItem);
				log.info("Get " + name + " nextid success, use time "
						+ (System.currentTimeMillis() - begintime));
				trans.setStatus(Message.SUCCESS);
				return tmpid;

			}
		} finally {
			trans.complete();
			unlockmemoryMap(name);
		}
	}

	public int addRouteItem(String value, String domain) throws LionException {
		Gson gson = new Gson();
		DatabaseTable tmptab = new DatabaseTable();
		try {
			tmptab = gson.fromJson(value, DatabaseTable.class);
		} catch (JsonParseException e) {
			log.warn("Json parse error when read " + domain
					+ " from zookeeper ", e);
			throw e;
		}
		RouteTableItem item = new RouteTableItem();
		item.setLocationTag(tmptab.getUrl());
		item.setUsernameTag(tmptab.getUser());
		item.setPasswordTag(tmptab.getPassword());
		item.setTablenameTag(tmptab.getTablename());
		String v = null;
		item.setDomain(tmptab.getDomain());
		v = routeConfig.getProperty(tmptab.getUrl());
		if (v.contains("mysql")) {
			item.setType("com.mysql.jdbc.Driver");
		} else{
			item.setType("mongo");
		}
		item.setLocation(v);
		addChangeUrl(tmptab.getUrl());
		v = routeConfig.getProperty(tmptab.getUser());
		item.setUsername(v);
		addChangeUsername(tmptab.getUser());
		v = routeConfig.getProperty(tmptab.getPassword());
		item.setPassword(v);
		addChangePassword(tmptab.getPassword());
		v = routeConfig.getProperty(tmptab.getTablename());
		item.setTablename(v);
		addChangeTableName(tmptab.getTablename());
		routeMap.put(domain, item);
		return 0;
	}

	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

}
