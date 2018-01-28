package com.nano.mongo_ras.tools;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.ejb.Asynchronous;
import javax.ejb.EJBTransactionRolledbackException;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.NamedQuery;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.transaction.RollbackException;
import javax.transaction.TransactionRolledbackException;

import org.apache.commons.lang.time.StopWatch;
import org.jboss.ejb3.annotation.TransactionTimeout;
import org.jboss.logging.Logger;

import com.nano.jpa.entity.Settings;
import com.nano.jpa.entity.Settings_;
import com.nano.jpa.entity.Subscriber;
import com.nano.jpa.entity.Subscriber_;
import com.nano.jpa.entity.ras.BorrowableAmount;
import com.nano.jpa.entity.ras.BorrowableAmount_;
import com.nano.jpa.entity.ras.SubscriberAssessment;
import com.nano.jpa.entity.ras.SubscriberAssessment_;
import com.nano.jpa.enums.SettingType;

/**
 * Manage database persistence and spooling via {@link PersistenceContext}.
 * 
 * @author segz
 *
 */

@Stateless
public class QueryManager {
	
	private Logger log = Logger.getLogger(getClass());

	private CriteriaBuilder criteriaBuilder ;

	@PersistenceContext(unitName = "nano-jpa")
	private EntityManager entityManager ;
	
	@Inject
	private PropertiesManager props ;

	@PostConstruct
	public void init(){
		criteriaBuilder = entityManager.getCriteriaBuilder();
	}
	
	/**
	 * Fetch {@link Settings} by name property.
	 * 
	 * @param name setting key
	 * @return {@link Settings}
	 */
	public Settings getSettingsByName(String name){

		CriteriaQuery<Settings> criteriaQuery = criteriaBuilder.createQuery(Settings.class);
		Root<Settings> root = criteriaQuery.from(Settings.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(Settings_.name), name));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No Setting instance found with name:" + name);
		}

		return null;
	}
	
	/**
	 * Create a new Setting instance or return existing.
	 * 
	 * @param name setting key
	 * @param value setting value
	 * @param description setting description
	 * @param settingType setting type
	 * @return {@link Settings} record
	 */
	public Settings createSettings(String name, 
			String value, String description, SettingType settingType){

		Settings settings = getSettingsByName(name);

		if (settings != null)
			return settings;

		settings = new Settings();
		settings.setDescription(description);
		settings.setName(name);
		settings.setType(settingType);
		settings.setValue(value);

		entityManager.persist(settings);
		
		return settings;
	}
	
	/**
	 * Fetch UN-ASSESSED or out dated assessments for {@link Subscriber} with {@link NamedQuery}.
	 * 
	 * @param maxResult fetch limit
	 * @return list of subscriber matriculation numbers
	 * @throws SQLException checked exception
	 * @throws TransactionRolledbackException checked exception
	 * @throws EJBTransactionRolledbackException checked exception
	 * @throws RollbackException checked exception
	 */
	@SuppressWarnings("unchecked")
	@TransactionTimeout(unit = TimeUnit.MINUTES, value = 20)
	public List<String> getMsisdnFromViewWithoutAssessmentOrWithOutdatedAssessment(int maxResult) 
			throws SQLException, TransactionRolledbackException, EJBTransactionRolledbackException, RollbackException {
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		Query query = entityManager.createNativeQuery("select msisdn from subscriber_ras_view");
		query.setMaxResults(maxResult);
		
		try {
			return query.getResultList();
		} catch (PersistenceException e) {
			// TODO Auto-generated catch block
			log.error("PersistenceException thrown. Transaction likely timed out");
		} finally {
			stopWatch.stop();
			log.info("Time taken to fetch list:" + stopWatch.getTime() + "ms");
		}
		
		return null;
	}
	
	/**
	 * Fetch UN-ASSESSED {@link Subscriber} with {@link NamedQuery}.
	 * 
	 * @param startPosition query index position
	 * @param maxResult fetch limit
	 * @return list of subscriber matriculation numbers
	 */
	@SuppressWarnings("unchecked")
	public List<String> getMsisdnFromView(int startPosition, 
			int maxResult){
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		String sql = new StringBuffer("select msisdn from ").append(props.getProperty("ras.query.table.name", "subscriber_eval")).append(" order by msisdn").toString() ;
		
		Query query = entityManager.createNativeQuery(sql);
		query.setFirstResult(startPosition);
		query.setMaxResults(maxResult);
		
		try {
			return query.getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		} finally {
			stopWatch.stop();
			log.info("Time taken to fetch list:" + stopWatch.getTime() + "ms");
		}
		
		return Collections.emptyList();
	}
	
	/**
	 * Refresh materialized view.
	 * 
	 */
	@Asynchronous
	@TransactionTimeout(unit = TimeUnit.MINUTES, value = 90)
	public void refreshMaterializedView(){
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		Query query = entityManager.createNativeQuery("refresh materialized view subscriber_assessment_proxy");
		
		try {
			query.executeUpdate();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		} finally {
			stopWatch.stop();
			log.info("time taken to refresh materialized view:" + stopWatch.getTime() + "ms");
		}
	}
	
	/**
	 * Fetch UN-ASSESSED {@link Subscriber} with {@link NamedQuery}.
	 * 
	 * @param startPosition query index position
	 * @param maxResult fetch limit
	 * @return list of subscriber matriculation numbers
	 */
	public List<String> getSubscriberWithoutAssessment(int startPosition, 
			int maxResult){
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		TypedQuery<String> typedQuery = entityManager.createNamedQuery("Subscriber.notInSubAssessment", String.class);
		typedQuery.setFirstResult(startPosition);
		typedQuery.setMaxResults(maxResult);
		
		try {
			return typedQuery.getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		} finally {
			stopWatch.stop();
			log.info("" + stopWatch.getTime());
		}
		
		return null;
	}
	
	/**
	 * Fetch {@link Subscriber} in paginated chunks.
	 * 
	 * @param index query index position
	 * @param size fetch limit
	 * @return list of subscriber matriculation numbers
	 */
	public List<String> getSubscribersMsisdn(int size){

		CriteriaQuery<String> criteriaQuery = criteriaBuilder.createQuery(String.class);
		Root<Subscriber> root = criteriaQuery.from(Subscriber.class);

		criteriaQuery.select(root.get(Subscriber_.msisdn));
		criteriaQuery.orderBy(criteriaBuilder.asc(root.get(Subscriber_.pk)));

		TypedQuery<String> typedQuery = entityManager.createQuery(criteriaQuery);
		//typedQuery.setFirstResult(index);
		typedQuery.setMaxResults(size);

		try {
			return typedQuery.getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("No subscriber instance found");
		}

		return null;
	}
	
	/**
	 * Fetch ordered {@link BorrowableAmount} list.
	 * 
	 * @return list of borrowableAmounts
	 */
	public List<BorrowableAmount> getBorrowableAmountListAsc(){

		CriteriaQuery<BorrowableAmount> criteriaQuery = criteriaBuilder.createQuery(BorrowableAmount.class);
		Root<BorrowableAmount> root = criteriaQuery.from(BorrowableAmount.class);

		criteriaQuery.select(root);
		criteriaQuery.orderBy(criteriaBuilder.asc(root.get(BorrowableAmount_.amount)));

		try {
			return entityManager.createQuery(criteriaQuery).getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("No borrowableAmount instance found");
		}

		return Collections.emptyList();
	}
	
	/**
	 * Fetch {@link Subscriber} by MSISDN property.
	 * 
	 * @param msisdn subscriber unique MSISDN
	 * @return {@link Subscriber}
	 */
	public Subscriber getSubscriberByMsisdn(String msisdn){

		CriteriaQuery<Subscriber> criteriaQuery = criteriaBuilder.createQuery(Subscriber.class);
		Root<Subscriber> root = criteriaQuery.from(Subscriber.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(Subscriber_.msisdn), msisdn));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriber instance was found with msisdn:" + msisdn);;
		}

		return null;
	}
	
	/**
	 * Fetch SubscriberAssessment by {@link Subscriber}.
	 * 
	 * @param subscriber subscriber details
	 * @return {@link SubscriberAssessment}
	 */
	public SubscriberAssessment getSubscriberAssessmentBySubscriber(Subscriber subscriber) {
		// TODO Auto-generated method stub

		CriteriaQuery<SubscriberAssessment> criteriaQuery = criteriaBuilder.createQuery(SubscriberAssessment.class);
		Root<SubscriberAssessment> root = criteriaQuery.from(SubscriberAssessment.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(SubscriberAssessment_.subscriber), subscriber));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriberAssessment instance found for subscriber:" + subscriber.getPk());
		}

		return null;
	}
	
	/**
	 * Format MSISDN to acceptable syntax.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return formatted MSISDN
	 */
	public String formatMisisdn(String msisdn){

		if (msisdn.startsWith("234"))
			msisdn = "0" + msisdn.substring(3, msisdn.length());

		if (msisdn.startsWith("+234"))
			msisdn = "0" + msisdn.substring(4, msisdn.length());

		if (!msisdn.startsWith("0"))
			msisdn = "0" + msisdn;

		return msisdn;
	}
	
	/**
	 * Fetch count of persisted entity instance.
	 * 
	 * @param clazz entity class
	 * @return count of records
	 */
	public <T> Long countOf(Class<T> clazz){

		CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
		Root<T> root = criteriaQuery.from(clazz);

		criteriaQuery.select(criteriaBuilder.count(root));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("No clazz:" + clazz.getName() + " instance found");
		}

		return 0L;
	}
	
	/**
	 * Fetch persisted entity instance by it {@link PrimaryKey}.
	 * 
	 * @param clazz entity class
	 * @param pk primary key
	 * @return persisted entity instance
	 */
	public <T> Object getByPk(Class<T> clazz, 
			long pk){

		CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(clazz);
		Root<T> root = criteriaQuery.from(clazz);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.and(criteriaBuilder.equal(root.get("pk"), pk)), 
				criteriaBuilder.equal(root.get("deleted"), false));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("No " + clazz.getCanonicalName() + " exists with the pk:" + pk);
		}

		return null;
	}

}