package com.nano.mongo_ras.tools;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import com.nano.jpa.entity.ras.BorrowableAmount;

import lombok.Getter;
import lombok.Setter;

/**
 * Application scoped bean for managing global Get and Set methods.
 * 
 * @author segz
 *
 */

@Getter
@Setter
@ApplicationScoped
public class ApplicationBean {
	
	private int fetchSize = 25000 ;
	private int breakTime = 5;
	private int amountPosition = 4;
	
	private boolean assessTopupFrequency = true ;
	private boolean assessTopupAmount = true ;
	private boolean assessAgeOnNetwork = true ;
	private boolean assessBlacklistStatus = true ;
	private boolean assessTarrifplan = true ;
	
	private List<BorrowableAmount> borrowableAmounts ;
	
}