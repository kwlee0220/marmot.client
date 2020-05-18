package marmot.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.common.collect.Lists;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import marmot.MarmotRuntime;
import marmot.command.MarmotClientCommand;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.kafka.KafkaImporterMain.CreateTopic;
import marmot.kafka.KafkaImporterMain.DeleteTopic;
import marmot.kafka.KafkaImporterMain.Import;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;
import utils.PicocliSubCommand;
import utils.stream.FStream;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="kafka_importer",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="Marmot Kafka-related commands",
		subcommands = {
			Import.class, CreateTopic.class, DeleteTopic.class,
		})
public class KafkaImporterMain extends MarmotClientCommand {
	private static final String DEF_TOPIC_NAME = "marmot_kafka_import";
	private static final String DEF_CONSUMER_ID = "marmot_importer";
	private static final int DEF_PARTITION_COUNT = 1;
	private static final int DEF_REPLICATION_FACTOR = 1;
	
	@Option(names= {"-broker"}, description="Kafka broker")
	private List<String> m_brokers = Lists.newArrayList();
	
	@Option(names= {"-topic"}, description="marmot importer topic name")
	private String m_topic = DEF_TOPIC_NAME;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		KafkaImporterMain cmd = new KafkaImporterMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	private String getKafkaBroker() {
		if ( m_brokers.size() > 0 ) {
			return FStream.from(m_brokers).join(',');
		}
		
		return MarmotConnector.getKafkaBroker()
								.getOrThrow(() -> new IllegalArgumentException("KafkaBroker is not specified"));
	}
	
	@Command(name="import", description="import DataSets published from Marmot topic")
	public static class Import extends PicocliSubCommand<MarmotRuntime> {
		@Option(names= {"-consumer"}, description="consumer id")
		private String m_consumerId = DEF_CONSUMER_ID;
		
		@Override
		protected void run(MarmotRuntime initialContext) throws Exception {
			KafkaImporterMain parent = (KafkaImporterMain)getParent();
			
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.getKafkaBroker());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, m_consumerId);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
						StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						ByteArrayDeserializer.class.getName());
			
			List<String> topics = Arrays.asList(parent.m_topic);
			ExecutorService exector = Executors.newCachedThreadPool();
			MarmotKafkaImporter importer = new MarmotKafkaImporter(initialContext, topics, props, exector);
			importer.start();
			importer.waitForDone();
			
			exector.shutdown();
			importer.get();
		}
	}
	
	static abstract class ZooKeeperCommand extends PicocliSubCommand<MarmotRuntime> {
		@Option(names= {"-zkhost"}, description="ZooKeeper host")
		private List<String> m_zkhost = Lists.newArrayList();
		
		protected String getZkHost() {
			if ( m_zkhost.size() > 0 ) {
				return FStream.from(m_zkhost).join(',');
			}
			
			return MarmotConnector.getZooKeeper()
									.getOrThrow(() -> new IllegalArgumentException("ZooKeeper host is not specified"));
		}
	}
	
	@Command(name="create", description="create a Marmot Kafka topic")
	public static class CreateTopic extends ZooKeeperCommand {
		@Option(names= {"-nparts"}, description="partition count")
		private int m_nparts = DEF_PARTITION_COUNT;
		
		@Option(names= {"-nreps"}, description="replication factor")
		private int m_nreps = DEF_REPLICATION_FACTOR;
		
		@Override
		protected void run(MarmotRuntime initialContext) throws Exception {
			int sessionTimeout = 15 * 1000;
			int connectionTimeout = 10 * 1000;
			String zkHost = getZkHost();
			
			KafkaImporterMain parent = (KafkaImporterMain)getParent();
			
			ZkUtils utils = null;
			try {
				utils = ZkUtils.apply(zkHost, sessionTimeout, connectionTimeout, false);
				AdminUtils.createTopic(utils, parent.m_topic, m_nparts, m_nreps, new Properties(),
										RackAwareMode.Safe$.MODULE$);
			}
			finally {
				if ( utils != null ) {
					utils.close();
				}
			}
		}
	}
	
	@Command(name="delete", description="delete a Marmot Kafka topic")
	public static class DeleteTopic extends ZooKeeperCommand {
		@Override
		protected void run(MarmotRuntime initialContext) throws Exception {
			String zkHost = getZkHost();
			KafkaImporterMain parent = (KafkaImporterMain)getParent();
			
			ZkClient client = new ZkClient(zkHost);
			try {
				String topicPath = ZkUtils.getTopicPath(parent.m_topic);
				client.deleteRecursive(topicPath);
			}
			finally {
				client.close();
			}
		}
	}
}
