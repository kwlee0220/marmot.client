package marmot.command;

import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteBindDataSetMain {
	private static final String DEFAULT_SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_bind_csv_dataset ");
		parser.addArgumentName("path|dataset_id");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("type", "name", "dataset type ('csv','custom','file', or 'dataset)", true);
		parser.addArgOption("dataset", "name", "target dataset name", true);
		parser.addArgOption("geom_col", "name", "default Geometry column", false);
		parser.addOption("h", "print usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
	
			String filePath = cl.getArgument(0);
			String typeStr = cl.getString("type");
			String dataset = cl.getString("dataset");
			String geomCol = cl.getOptionString("geom_col").getOrNull(); 
			String srid = cl.getOptionString("srid").getOrElse(DEFAULT_SRID);
			
			GeometryColumnInfo gcinfo = ( geomCol != null )
										? new GeometryColumnInfo(geomCol, srid) : null;
	
			DataSetType type;
			if ( "dataset".equals(typeStr) ) {
				DataSet srcDs = marmot.getDataSet(filePath);
				filePath = srcDs.getHdfsPath();
				type = srcDs.getType();
				
				if ( srcDs.hasGeometryColumn() ) {
					gcinfo = srcDs.getGeometryColumnInfo();
				}
			}
			else {
				if ( geomCol != null && srid == null ) {
					throw new IllegalArgumentException("srid is not specified");
				}
				type = DataSetType.fromString(typeStr.toUpperCase());
			}
	
			marmot.deleteDataSet(dataset);
			if ( gcinfo != null ) {
				marmot.bindExternalDataSet(dataset, filePath, type, gcinfo);
			}
			else {
				marmot.bindExternalDataSet(dataset, filePath, type);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
