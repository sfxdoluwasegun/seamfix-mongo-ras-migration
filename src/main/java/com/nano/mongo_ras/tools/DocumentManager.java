package com.nano.mongo_ras.tools;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ejb.Stateless;
import javax.inject.Inject;
import org.bson.Document;
import org.jboss.logging.Logger;

import com.mongodb.client.MongoCollection;
import com.nano.mongo_ras.documents.AppDocuments;
import com.nano.mongo_ras.documents.SubscriberHistory;
import com.nano.mongo_ras.documents.SubscriberState;

@Stateless
public class DocumentManager {

	private Logger log = Logger.getLogger(getClass());

	@Inject
	private MongoManager mongoManager ;

	/**
	 * Fetch SubscriberState document by MSISDN.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return SubscriberState document
	 */
	public Document getSubscriberStateByMsisdn(String msisdn){

		MongoCollection<Document> mongoCollection = mongoManager.getCollectionConnection(AppDocuments.subscriber_state.name());

		Document filter = new Document(SubscriberState.msisdn.name(), msisdn);

		Document subscriberState = mongoCollection.find(filter).first();
		if (subscriberState != null)
			return subscriberState;

		return null;
	}

	/**
	 * Fetch SubscriberHistory document by MSISDN property.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return list of SubscriberHistory documents
	 */
	public List<Document> getSubscriberHistoryByMsisdn(String msisdn){

		MongoCollection<Document> mongoCollection = mongoManager.getCollectionConnection(AppDocuments.subscriber_history.name());

		Document filter = new Document(SubscriberHistory.msisdn.name(), msisdn);

		List<Document> subscriberHistories = mongoCollection.find(filter).into(new ArrayList<>());
		if (subscriberHistories != null)
			return subscriberHistories;

		return Collections.emptyList();
	}

	/**
	 * Fetch earliest Time stamp from SubscriberHistory documents by MSIDN property.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return time-stamp retrieved from document
	 */
	public Timestamp getEarliestSubscriberHistoryTimeByMsisdn(String msisdn){

		MongoCollection<Document> mongoCollection = mongoManager.getCollectionConnection(AppDocuments.subscriber_history.name());

		Document filter = new Document(SubscriberHistory.msisdn.name(), msisdn);
		Document sort = new Document(SubscriberHistory.recharge_time.name(), 1);
		Document subscriberHistory = null;

		try {
			subscriberHistory = mongoCollection.find(filter).sort(sort).limit(1).first();
			return (Timestamp) subscriberHistory.get(SubscriberHistory.recharge_time.name());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}

		return null;
	}

	/**
	 * Create {@link SubscriberState}.
	 * 
	 * @param msisdn subscriber unique MSISDN
	 * @param subscribers currentBalance
	 */
	public void createSubscriberState(String msisdn, 
			BigDecimal currentBalance) {
		// TODO Auto-generated method stub

		MongoCollection<Document> mongoCollection = mongoManager.getCollectionConnection(AppDocuments.subscriber_state.name());

		Document filter = new Document(SubscriberState.msisdn.name(), msisdn);
		Document subscriberState = mongoCollection.find(filter).first();

		new Document(SubscriberState.active_status.name(), true)
		.append(SubscriberState.blacklisted.name(), false)
		.append(SubscriberState.current_balance.name(), currentBalance.doubleValue())
		.append(SubscriberState.last_updated.name(), Timestamp.valueOf(LocalDateTime.now()))
		.append(SubscriberState.msisdn.name(), msisdn)
		.append(SubscriberState.pay_type.name(), com.nano.jpa.enums.PayType.PREPAID.getDescription());
		
		mongoCollection.insertOne(subscriberState);
	}

}