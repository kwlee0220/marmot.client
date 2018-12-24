package marmot.command;

import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteDeleteDataSetMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="dataset-id (or directory-id)", description={"dataset id to display"})
	private String m_target;
	
	@Option(names={"-r"}, description="delete all datasets in subdirectories recursively")
	private boolean m_recursive;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteDeleteDataSetMain cmd = new RemoteDeleteDataSetMain();
		CommandLine commandLine = new CommandLine(cmd);
		commandLine.parse(args);
		
		if ( commandLine.isUsageHelpRequested() ) {
			commandLine.usage(System.out, Ansi.OFF);
		}
		else {
			cmd.run();
		}
	}

	@Override
	public void run() {
		try {
			PBMarmotClient marmot = m_connector.connect();
			
			if ( m_recursive ) {
				marmot.deleteDir(m_target);
			}
			else {
				marmot.deleteDataSet(m_target);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
}
