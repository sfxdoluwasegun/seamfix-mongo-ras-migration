package com.nano.mongo_ras.tools;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import javax.ejb.AccessTimeout;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.jboss.logging.Logger;

import com.nano.jpa.entity.Subscriber;
import com.nano.jpa.entity.ras.SubscriberAssessment;
import com.nano.jpa.enums.PayType;

@Singleton
@Lock(LockType.READ)
@AccessTimeout(unit = TimeUnit.MINUTES, value = 3)
public class SingletonQueries {
	
	private Logger log = Logger.getLogger(getClass());

	@PersistenceContext(unitName = "nano-jpa")
	private EntityManager entityManager ;
	
	@Inject
	private QueryManager qm ;
	
	/**
	 * Merge the state of the given entity into the current {@link PersistenceContext}.
	 * 
	 * @param entity entity instance
	 * @return the managed instance that the state was merged to
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public <T> Object updateWithNewTransaction(T entity){

		entityManager.merge(entity);
		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}

		return null;
	}
	
	/**
	 * Merge the state of the given entity into the current {@link PersistenceContext}.
	 * 
	 * @param entity
	 * @return the managed instance that the state was merged to
	 */
	public <T> Object update(T entity){

		entityManager.merge(entity);
		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}

		return null;
	}
	
	/**
	 * Persist entity and add entity instance to {@link EntityManager}.
	 * 
	 * @param entity entity instance
	 * @return persisted entity instance
	 */
	public <T> Object create(T entity){

		entityManager.persist(entity);

		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}
		return null;
	}
	
	/**
	 * Creates or fetches a unique {@link Subscriber} record.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return {@link Subscriber}
	 */
	@Lock(LockType.WRITE)
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public synchronized Subscriber createSubscriber(String msisdn){

		Subscriber subscriber = qm.getSubscriberByMsisdn(qm.formatMisisdn(msisdn));

		if (subscriber != null)
			return subscriber;

		subscriber = new Subscriber();
		subscriber.setInDebt(false);
		subscriber.setAutoRecharge(false);
		subscriber.setMsisdn(qm.formatMisisdn(msisdn));

		return (Subscriber) create(subscriber);
	}
	
	/**
	 * Create a fresh SubscriberAssessment.
	 * 
	 * @param subscriber
	 * @param subscriberState
	 * @return {@link SubscriberAssessment}
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public SubscriberAssessment createNewAssessment(Subscriber subscriber, 
			PayType payType){

		SubscriberAssessment subscriberAssessment = qm.getSubscriberAssessmentBySubscriber(subscriber);
		if (subscriberAssessment != null)
			return subscriberAssessment;

		subscriberAssessment = new SubscriberAssessment();
		subscriberAssessment.setAgeOnNetwork(0);
		subscriberAssessment.setInDebt(subscriber.isInDebt());
		subscriberAssessment.setLastProcessed(Timestamp.valueOf(LocalDateTime.now()));
		subscriberAssessment.setNumberOfTopUps(0);
		subscriberAssessment.setSubscriber(subscriber);
		subscriberAssessment.setTopUpDuration(0);
		subscriberAssessment.setTopUpValueDuration(0);
		subscriberAssessment.setTotalTopUpValue(0);

		if(payType != null)
			subscriberAssessment.setTariffPlan(payType);

		return (SubscriberAssessment) create(subscriberAssessment);
	}

}