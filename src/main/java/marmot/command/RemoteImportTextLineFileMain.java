package marmot.command;

import java.io.File;
import java.nio.charset.Charset;

import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.externio.text.ImportTextFile;
import marmot.externio.text.TextLineParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteImportTextLineFileMain {
	private static final String META_FILE_NAME = "_meta.json";
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		StopWatch watch = StopWatch.start();
		
		CommandLineParser parser = new CommandLineParser("mc_import_textfile ");
		parser.addArgumentName("path|dir");
		parser.addArgOption("dataset", "id", "target dataset id", true);
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("geom_col", "name", "default Geometry column", false);
		parser.addArgOption("srid", "code", "EPSG code for result dataset", false);
		parser.addArgOption("charset", "charset", "Character encoding (default: utf-8)", false);
		parser.addArgOption("comment", "str", "comment line prefix", false);
		parser.addArgOption("glob", "glob", "path matcher", true);
		parser.addArgOption("block_size", "size", "block size", false);
		parser.addOption("f", "force to create a new dataset", false);
		parser.addArgOption("report_interval", "count", "progress report interval", false);
		parser.addOption("h", "show usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("h") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		File file = new File(cl.getArgument(0));
		String dsId = cl.getString("dataset");
		String geomCol = cl.getOptionString("geom_col").getOrNull();
		String srid = cl.getOptionString("srid").getOrNull();
		Charset charset = Charset.forName(cl.getOptionString("charset").getOrElse("utf-8"));
		FOption<String> comment = cl.getOptionString("comment");
		String glob = cl.getString("glob");
		long blkSize = cl.getOptionString("block_size")
							.map(UnitUtils::parseByteSize)
							.getOrElse(-1L);
		boolean force = cl.hasOption("f");
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		ImportParameters params = ImportParameters.create()
												.setDatasetId(dsId)
												.setGeometryColumnInfo("the_geom", srid)
												.setBlockSize(blkSize)
												.setReportInterval(reportInterval)
												.setForce(force);
		TextLineParameters txtParams = TextLineParameters.parameters()
														.glob(glob)
														.charset(charset);
		comment.ifPresent(txtParams::commentMarker);
		ImportIntoDataSet importFile = new ImportTextFile(file, txtParams, params);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		importFile.run(marmot);
		
		marmot.disconnect();
	}
}
