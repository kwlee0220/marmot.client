package marmot.command;

import marmot.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_move", description="move a dataset to another directory")
public class RemoteMoveDataSetMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(index="0", paramLabel="src_path", description={"path to the dataset (or directory) to move"})
	private String m_src;
	
	@Parameters(index="1", paramLabel="dest_path", description={"path to the destination path"})
	private String m_dest;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteMoveDataSetMain cmd = new RemoteMoveDataSetMain();
		CommandLine commandLine = new CommandLine(cmd);
		
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
	public void run() {
		try {
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = m_connector.connect();
	
			DataSet srcDs = marmot.getDataSet(m_src);
			marmot.moveDataSet(srcDs.getId(), m_dest);
		}
		catch ( Exception e ) {
			System.err.println("" + e);
		}
	}
}
