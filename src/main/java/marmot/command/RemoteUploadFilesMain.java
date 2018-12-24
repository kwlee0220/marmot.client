package marmot.command;

import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_upload_files",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="upload local files into HDFS")
public class RemoteUploadFilesMain extends UploadFilesCommand {
	@Mixin protected MarmotConnector m_connector;
	
	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteUploadFilesMain cmd = new RemoteUploadFilesMain();
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