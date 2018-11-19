package marmot.command;

import marmot.DataSet;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintSchemaMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_schema ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("h", "help usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			String dsId = cl.getArgument("dataset");
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
	
			DataSet info = marmot.getDataSet(dsId);
			
			System.out.println("TYPE     : " + info.getType());
			if ( info.getRecordCount() > 0 ) {
				System.out.println("COUNT    : " + info.getRecordCount());
			}
			else {
				System.out.println("COUNT    : unknown");
			}
			System.out.println("SIZE     : " + UnitUtils.toByteSizeString(info.length()));
			if ( info.hasGeometryColumn() ) {
				System.out.println("GEOMETRY : " + info.getGeometryColumnInfo().name());
				System.out.println("SRID     : " + info.getGeometryColumnInfo().srid());
			}
			System.out.println("HDFS PATH: " + info.getHdfsPath());
			SpatialIndexInfo idxInfo = info.getDefaultSpatialIndexInfoOrNull();
			System.out.printf("SPATIAL INDEX: %s%n", (idxInfo != null)
														? idxInfo.getHdfsFilePath() : "none");
			System.out.println("COLUMNS  :");
			info.getRecordSchema().getColumnAll()
					.stream()
					.forEach(c -> System.out.println("\t" + c));
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
