package marmot.geo.command;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListSpatialClusters {
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_list_spatial_clusters ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("help", "show usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		String dsId = cl.getArgument("dataset");
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Plan program = marmot.planBuilder("list_spatial_clusters")
								.loadSpatialClusterIndexFile(dsId)
								.project("*-{bounds,value_envelope}")
								.build();
		try ( RecordSet rset = marmot.executeLocally(program) ) {
			rset.forEach(r -> printIndexEntry(r));
		}
	}
	
	private static final void printIndexEntry(Record record) {
		String packId = record.getString("pack_id");
		int blockNo = record.getInt("block_no", -1);
		String quadKey = record.getString("quad_key");
		long count = record.getLong("count", -1);
		String start = UnitUtils.toByteSizeString(record.getLong("start", -1));
		String len = UnitUtils.toByteSizeString(record.getLong("length", -1));
		
		System.out.printf("pack_id=%s, block_no=%02d, quad_key=%s, count=%d, start=%s, length=%s%n",
							packId, blockNo, quadKey, count, start, len);
	}
}
