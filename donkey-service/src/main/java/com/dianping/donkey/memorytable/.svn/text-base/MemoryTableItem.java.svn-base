package com.dianping.donkey.memorytable;


/**
 * define a item of memorytable
 * 
 * @author peng.hu
 * 
 */
public class MemoryTableItem {
	private int pos = 0;
	private boolean posHasChange;
	private String name;
	// private AtomicLong nextId;
	private long nextId[];
	private long lastNum[];
	private Boolean increment;
	private boolean asThreadStart;

	public MemoryTableItem() {
		this.posHasChange = true;
		asThreadStart = false;
		this.pos = 0;
		this.nextId = new long[2];
		this.lastNum = new long[2];
	}
	public boolean isPosHasChange() {
		return posHasChange;
	}

	public void setPosHasChange(boolean posHasChange) {
		this.posHasChange = posHasChange;
	}
	public boolean getAsThreadStart() {
		return this.asThreadStart;
	}

	public void setAsThreadStart(boolean asThreadStart) {
		this.asThreadStart = asThreadStart;
	}

	public long getNextId() {
		return this.nextId[pos]++;
	}

	public void setNextId(long nextId) {
		this.nextId[pos] = nextId;
	}
	
	public void setOtherNextId(long nextId){
		this.nextId[(pos+1)%2] = nextId;
	}

	private String type;
	private String location;

	// public MemoryTableItem() {
	// nextId = new AtomicLong();
	// }
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	// public long getNextId() {
	// return nextId.getAndIncrement();
	// }
	// public void setNextId(long nextId) {
	// this.nextId.set(nextId);
	// }
	public long getLastNum() {
		return lastNum[pos];
	}

	public void setLastNum(long lastNum) {
		this.lastNum[pos] = lastNum;
	}
	public void setOtherLastNum(long lastNum) {
		this.lastNum[(pos+1)%2] = lastNum;
	}

	public Boolean getIncrement() {
		return increment;
	}

	public void setIncrement(Boolean increment) {
		this.increment = increment;
	}

	public void posChange() {
		this.pos = (this.pos + 1) % 2;
	}
}