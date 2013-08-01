package com.dianping.donkey;

/**
 * 
 * @author peng.hu
 * 
 */
public interface DonkeyService {
	/**
	 * 
	 * @param domain name of the domain
	 * @param key name of the key
	 * @return allocated Id
	 */
	long nextID(String domain, String key);

	public enum Status {
		INVALIDKEY(-2), GETFAIL(-1);
		
		private int code;
		
		private Status(int code){
			this.code = code;
		}
		
		public int code(){
			return code;
		}
	}
}
