package com.dianping.donkey;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;

/**
 * 
 * @author peng.hu
 * 
 */
final public class DonkeyClient {
	private static final String DEFAULT_HEARTBEATINTERVAL = "300000";
	private static final Logger log = Logger.getLogger(DonkeyClient.class);

	public static long getSleepTime() {
		return SleepTime;
	}

	public static void setSleepTime(long sleepTime) {
		SleepTime = sleepTime;
	}

	private final static DonkeyClient instance = new DonkeyClient();
	private DonkeyService donkeyService = null;
	private ConfigCache config;
	private static long SleepTime;
	private static volatile boolean keepAliveThread = false;

	/**
	 * use singleton mode to get donkeyclient
	 * 
	 * @return a donkeyclient
	 */
	public static synchronized DonkeyClient getInstance()
			throws DonkeyClientException {
		if (!keepAliveThread) {
			Thread t = new Thread(new Runnable() {
				public void run() {
					while (true) {
						try {
							instance.getNextId("group", "test");
						} catch (DonkeyClientException e) {

						}
						try {
							Thread.sleep(SleepTime);
						} catch (InterruptedException e) {
							try {
								throw e;
							} catch (InterruptedException e1) {

							}
						}
					}
				}
			});
			t.setDaemon(true);
			t.setName("HeartbeatThread");
			t.start();
			keepAliveThread = true;
		}
		return instance;
	}

	/**
	 * init a donkeyclient
	 * 
	 * @throws LionException
	 */
	private DonkeyClient() {
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"config/spring/donkey-client.xml");
		donkeyService = (DonkeyService) ctx.getBean("donkeyService");
		try {
			config = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
			String sleepTimeStr = config
					.getProperty("donkey-service.heartbeatInterval");
			if (StringUtils.isEmpty(sleepTimeStr)
					|| !StringUtils.isNumeric(sleepTimeStr)) {
				sleepTimeStr = DEFAULT_HEARTBEATINTERVAL;
			}
			SleepTime = Long.parseLong(sleepTimeStr);
			config.addChange(new ConfigChange() {
				// register call back function
				public void onChange(String key, String value) {
					if (key != null
							&& key.equals("donkey-service.heartbeatInterval")) {
						if (StringUtils.isEmpty(value)
								|| !StringUtils.isNumeric(value)) {
							setSleepTime(Long
									.parseLong(DEFAULT_HEARTBEATINTERVAL));
						} else{
							setSleepTime(Long.parseLong(value));
						}
						log.info("Heartbeatinterval has change to " + value);
					}
				}
			});
		} catch (NumberFormatException e) {
		} catch (LionException e) {
		}

	}

	/**
	 * get an Id for donkey service
	 * 
	 * @param domain
	 *            name of the domain
	 * @param key
	 *            name of the key
	 * @return allocated id
	 * @throws DonkeyClientException
	 *             if the id could not obtained
	 */
	public long getNextId(String domain, String key)
			throws DonkeyClientException {
		if (domain == null || key == null) {
			throw new DonkeyClientException("Input invalid domain or key.");
		}

		long nextID = -1;
		int i = 0;
		String donkeyDomain = "donkey-service." + domain;
		while (i++ < 3) {
			try {
				nextID = donkeyService.nextID(donkeyDomain, key);
				if (nextID >= 0) {
					return nextID;
				} else if (nextID == DonkeyService.Status.INVALIDKEY.code()) {
					throw new DonkeyClientException("Input invalid domain.");
				} else {
					continue;
				}
			} catch (Exception e) {
				log.warn("Connect time out!", e);
			}
		}
		throw new DonkeyClientException("Get domain: " + domain + " key: "
				+ key + " fail.");
	}

	public static void main(String[] args) throws DonkeyClientException {
		DonkeyClient d = new DonkeyClient();
		d = d.getInstance();
		if (d.getNextId("group", "hupeng") >= 0){
			System.out.println("Start Success!");
		}
		else{
			System.out.println("Start Fail!");
		}
	}

}
