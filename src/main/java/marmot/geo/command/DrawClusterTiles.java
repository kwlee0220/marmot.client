package marmot.geo.command;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrawClusterTiles {
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_draw_cluster_tiles ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("output", "path", "shapefile path", true);
		parser.addArgOption("charset", "name", "character encoding (default: euc-kr)", false);
		parser.addOption("f", "delete the output file if it exists already", false);
		parser.addOption("value", "draw value envelope", false);
		parser.addOption("help", "show usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		String dsId = cl.getArgument("dataset");
		String output = cl.getString("output");
		String charsetName = cl.getOptionString("charset").getOrElse("euc-kr");
		boolean force = cl.hasOption("f");
		boolean value = cl.hasOption("value");
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		String toGeom = (value) ? "ST_GeomFromEnvelope(value_envelope)"
								: "ST_GeomFromEnvelope(bounds)";
		
		Plan plan = marmot.planBuilder("read_cluster_index")
							.loadSpatialClusterIndexFile(dsId)
							.defineColumn("the_geom:polygon", toGeom)
							.project("the_geom,pack_id,quad_key,count,length")
							.build();
		
		if ( force ) {
			new File(output).delete();
		}
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			ShapefileParameters params = ShapefileParameters.create()
														.charset(Charset.forName(charsetName));
			ExportRecordSetAsShapefile exporter = new ExportRecordSetAsShapefile(rset, "EPSG:4326",
																				output, params);
			exporter.setForce(true);
			exporter.start().waitForDone();
		}
	}
}
