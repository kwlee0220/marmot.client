package marmot.geo.command;

import static marmot.DataSetOption.*;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangeQuery {
	//POLYGON ((126.974383 37.566222, 126.982998 37.566222, 126.982998 37.562395, 126.982998 37.566222, 126.974383 37.566222))
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_range_query ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("dataset", "name", "dataset id", true);
		parser.addArgOption("op", "op", "spatial relation (default: intersects)", false);
		parser.addArgOption("key", "wkt", "key geometry", true);
		parser.addArgOption("output", "name", "output dataset id", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		String dsId = cl.getString("dataset");
		String wkt = cl.getString("key");
		String op = cl.getOptionString("op").getOrElse("intersects").toLowerCase();
		String outDs = cl.getString("output");
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		DataSet ds = marmot.getDataSet(dsId);
		if ( !ds.hasGeometryColumn() ) {
			System.err.println("target dataset does not have default Geometry column: "
								+ "id=" + dsId);
			System.exit(-1);
		}
		
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Geometry key = GeoClientUtils.fromWKT(wkt);
		key = CoordinateTransform.get("EPSG:4326", gcInfo.srid()).transform(key);

		SpatialRelation rel = SpatialRelation.parse(op);
		Plan plan = marmot.planBuilder("range_query")
							.query(dsId, rel, key)
							.store(outDs)
							.build();
		marmot.createDataSet(outDs, plan, GEOMETRY(gcInfo), FORCE);
	}
}
