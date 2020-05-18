package marmot.command.explorer;

import java.awt.EventQueue;

import marmot.command.MarmotClientCommand;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_explorer",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="Swing-based dataset explorer")
public class RemoteDataSetExplorerMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteDataSetExplorerMain cmd = new RemoteDataSetExplorerMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	@Override
	public void run() {
		try {
			PBMarmotClient marmot = getInitialContext();

			EventQueue.invokeLater(new Runnable() {
				public void run() {
					try {
						DataSetExplorer frame = new DataSetExplorer(marmot);
						frame.setVisible(true);
						frame.init();
					}
					catch ( Exception e ) {
						e.printStackTrace();
					}
				}
			});
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
		}
	}
}
