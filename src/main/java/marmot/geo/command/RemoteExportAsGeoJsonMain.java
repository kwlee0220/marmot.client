package marmot.geo.command;

import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.ParseException;

import marmot.command.MarmotClientCommands;
import marmot.externio.ExternIoUtils;
import marmot.externio.geojson.ExportAsGeoJson;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineException;
import utils.CommandLineParser;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteExportAsGeoJsonMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);

		CommandLineParser parser = new CommandLineParser("mc_export_geojson ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: $MARMOT_HOST)", false);
		parser.addArgOption("port", "number", "marmot server port (default: $MARMOT_PORT)", false);
		parser.addArgOption("output", "path", "output CSV file path, 'stdout', or 'stderr'", false);
		parser.addArgOption("charset", "code", "character set code (default: utf-8)", false);
		parser.addOption("pretty", "print pretty GeoJSON", false);
		parser.addOption("wgs84", "use WGS84 coordinate system", false);
		parser.addOption("h", "print usage", false);

		CommandLine cl = null;
		try {
			cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}

			String dsId = cl.getArgument(0);
			Charset charset = cl.getOptionString("charset")
								.map(Charset::forName)
								.getOrElse(StandardCharsets.UTF_8);
			boolean wgs84 = cl.hasOption("wgs84");
			boolean pretty = cl.hasOption("pretty");
			
			ExportAsGeoJson export = new ExportAsGeoJson(dsId)
										.wgs84(wgs84)
										.printPrinter(pretty);
			
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			FOption<String> output = cl.getOptionString("output");
			BufferedWriter writer = ExternIoUtils.toWriter(output, charset);
			long count = export.run(marmot, writer);
			
			System.out.printf("done: %d records%n", count);
		}
		catch ( ParseException | CommandLineException e ) {
			System.err.println(e);
			if ( cl != null ) {
				cl.exitWithUsage(-1);
			}
		}
	}
}
