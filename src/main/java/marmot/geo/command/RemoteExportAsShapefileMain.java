package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_export_shapefile",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="export a dataset as Shapefile format")
public class RemoteExportAsShapefileMain extends ExportAsShapefileCommand {
	@Mixin private MarmotConnector m_connector;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteExportAsShapefileMain cmd = new RemoteExportAsShapefileMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				PBMarmotClient marmot = cmd.m_connector.connect();
				cmd.accept(marmot);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
