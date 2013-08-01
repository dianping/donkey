package com.dianping.donkeytest;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;

import com.dianping.donkey.DonkeyService;

public class DonkeyClienttest {

	 private Mockery context = new Mockery() {
		    {
		        setImposteriser(ClassImposteriser.INSTANCE);
		    }
		    };

	@Test
	public void testgetnextid() throws Exception {
		final DonkeyService ds = context.mock(DonkeyService.class);
		//DonkeyClient dc = DonkeyClient.getInstance();
		final long value = 1000;
		context.checking(new Expectations() {
			{
				try {
					oneOf(ds).nextID("donkey-service.group.test",
							"DP_Donkey");
					will(returnValue(value));
				} catch (Exception e) {
					System.out.println("fail");
				}
			}
		});
		Assert.assertEquals(1000,ds.nextID("donkey-service.group", "test"));
//		Field privateStringField1 = DonkeyClient.class
//				.getDeclaredField("donkeyService");
//		privateStringField1.setAccessible(true);
	//	privateStringField1.set(dc, ds);
	//	Assert.assertEquals(1000,dc.getNextId("group", "test"));
	}
}
