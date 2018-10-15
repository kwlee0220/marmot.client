package marmot.command;

import java.util.List;

import marmot.DataSet;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListDataSetsMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_dir ");
		parser.addArgumentName("dir");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: $MARMOT_HOST)", false);
		parser.addArgOption("port", "number", "marmot server port (default: $MARMOT_PORT)", false);
		parser.addOption("r", "list all descendant datasets", false);
		parser.addOption("l", "list in detail", false);
		parser.addOption("h", "print usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
	
			String dirPath = cl.getArgumentCount() > 0 ? cl.getArgument("dir") : null;
			boolean recursive = cl.hasOption("r");
			boolean listAll = cl.hasOption("l");
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			List<DataSet> dsList;
			if ( dirPath != null ) {
				dsList = marmot.getDataSetAllInDir(dirPath, recursive);
			}
			else {
				dsList = marmot.getDataSetAll();
			}
			
			for ( DataSet ds: dsList ) {
				System.out.print(ds.getId());
				
				if ( listAll ) {
					System.out.printf(" %s %s", ds.getType(), ds.getHdfsPath());
					if ( ds.hasGeometryColumn() ) {
						System.out.printf(" %s", ds.getGeometryColumnInfo());
						
						SpatialIndexInfo idxInfo = ds.getDefaultSpatialIndexInfoOrNull();
						if ( idxInfo != null ) {
							System.out.printf("(clustered)");
						}
					}
				}
				System.out.println();
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
