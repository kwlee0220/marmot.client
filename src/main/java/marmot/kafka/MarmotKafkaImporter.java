package marmot.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.proto.RecordProto;
import marmot.protobuf.PBRecordProtos;
import marmot.rset.PipedRecordSet;
import utils.Throwables;
import utils.async.AbstractThreadedExecution;
import utils.func.KeyValue;
import utils.stream.FStream;
import utils.thread.CamusExecutor;
import utils.thread.CamusExecutorImpl;
import utils.thread.RecurringSchedule;
import utils.thread.RecurringWork;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotKafkaImporter extends AbstractThreadedExecution<Long> {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotKafkaImporter.class);
	
	private static final int PIPE_LENGTH = 16;
	private static final long POLL_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(100);
	private static final long UPLOADER_TIMEOUT = 5;
	private static final long CACHE_CLEANER_INTERVAL = TimeUnit.SECONDS.toMillis(1);
	
	private final MarmotRuntime m_marmot;
	private final List<String> m_topics;
	private final Properties m_kafkaProps;
	private final CamusExecutor m_executor;
	private final LoadingCache<String,Uploader> m_uploaders;
	private final RecurringSchedule m_cacheCleaner;
	
	public MarmotKafkaImporter(MarmotRuntime marmot, List<String> topics, Properties props,
								ExecutorService exector) {
		m_marmot = marmot;
		m_topics = topics;
		m_kafkaProps = props;
		m_executor = new CamusExecutorImpl(exector);
		setLogger(s_logger);

		m_uploaders = CacheBuilder.newBuilder()
								.expireAfterAccess(UPLOADER_TIMEOUT, TimeUnit.SECONDS)
								.removalListener(this::onDataSetRemoved)
								.build(new CacheLoader<String,Uploader>() {
									@Override
									public Uploader load(String dsId) throws Exception {
										DataSet ds = m_marmot.getDataSet(dsId);
										Uploader uploader = new Uploader(ds, getLogger());
										if ( getLogger().isInfoEnabled() ) {
											getLogger().info("create a uploader: ds={}", ds.getId());
										}
										m_executor.submit(uploader);
										
										return uploader;
									}
								});
		m_cacheCleaner = m_executor.createScheduleWithFixedDelay(new IdleUploaderChecker(),
												CACHE_CLEANER_INTERVAL, CACHE_CLEANER_INTERVAL);
		try {
			m_cacheCleaner.start();
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	protected Long executeWork() throws InterruptedException, CancellationException, Exception {
		try ( KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(m_kafkaProps) ) {
			consumer.subscribe(m_topics);
			
			while ( true ) {
				ConsumerRecords<String,byte[]> records = consumer.poll(POLL_TIMEOUT);
				for ( KeyValue<String,List<byte[]>> keyedList: FStream.from(records)
																	.groupByKey(ConsumerRecord::key,
																				ConsumerRecord::value) ) {
					Uploader loader = m_uploaders.get(keyedList.key());
					FStream.from(keyedList.value()).forEach(loader::supply);
				}
				
				if ( isCancelRequested() ) {
					m_cacheCleaner.stop(true);
					m_uploaders.invalidateAll();
					
					throw new CancellationException();
				}
			}
		}
	}
	
	private void onDataSetRemoved(RemovalNotification<String,Uploader> noti) {
		String dsId = noti.getKey();
		
		getLogger().info("end-of-upload: dataset={}", dsId);
		noti.getValue().endOfSupply();
	}
	
	private class IdleUploaderChecker implements RecurringWork {
		@Override public void onStarted(RecurringSchedule schedule) throws Throwable { }
		@Override public void onStopped() {
			getLogger().debug("stopped: {}.cacheCleaner", MarmotKafkaImporter.class);
		}

		@Override
		public void perform() throws Exception {
			m_uploaders.cleanUp();
		}
	}
	
	private static class Uploader implements Callable<Long> {
		private final DataSet m_ds;
		private final RecordSchema m_schema;
		private final PipedRecordSet m_pipe;
		private final Logger m_logger;
		
		private Uploader(DataSet ds, Logger logger) {
			m_ds = ds;
			m_schema = ds.getRecordSchema();
			m_pipe = new PipedRecordSet(m_schema, PIPE_LENGTH);
			m_logger = logger;
		}

		@Override
		public Long call() throws Exception {
			long cnt = m_ds.append(m_pipe, "kafka");
			m_logger.info("uploaded: ds={}, count={}", m_ds.getId(), cnt);
			
			return cnt;
		}
		
		private synchronized void supply(byte[] bytes) {
			try {
				Record record = PBRecordProtos.fromProto(RecordProto.parseFrom(bytes), m_schema);
				m_pipe.supply(record);
			}
			catch ( Exception e ) {
				throw new RecordSetException("" + e);
			}
		}
		
		public synchronized void endOfSupply() {
			m_pipe.endOfSupply();
		}
	}
}
