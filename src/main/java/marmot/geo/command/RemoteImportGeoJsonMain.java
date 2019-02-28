package marmot.geo.command;

import java.io.File;

import io.vavr.CheckedRunnable;
import marmot.command.ImportParameters;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.command.UsageHelp;
import marmot.externio.geojson.GeoJsonParameters;
import marmot.externio.geojson.ImportGeoJson;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_import_geojson",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="import GeoJson files into a dataset")
public class RemoteImportGeoJsonMain implements CheckedRunnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private Params m_params;
	@Mixin private ImportParameters m_importParams;
	@Mixin private GeoJsonParameters m_gjsonParams;
	@Mixin private UsageHelp m_help;
	
	private static class Params {
		@Parameters(paramLabel="path", index="0", arity="1..1",
					description={"path to the target geojson files (or directories)"})
		private String m_path;
	}

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteImportGeoJsonMain cmd = new RemoteImportGeoJsonMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void run() throws Exception {
		PBMarmotClient marmot = m_connector.connect();
		
		StopWatch watch = StopWatch.start();
		
		if ( m_importParams.getGeometryColumnInfo().isAbsent() ) {
			throw new IllegalArgumentException("Option '-geom_col' is missing");
		}
		
		File gjsonFile = new File(m_params.m_path);
		ImportGeoJson importFile = ImportGeoJson.from(gjsonFile, m_gjsonParams, m_importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							m_importParams.getDataSetId(), count, watch.getElapsedMillisString(),
							velo);
	}
}
