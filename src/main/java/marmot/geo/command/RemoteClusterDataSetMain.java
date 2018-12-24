package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.command.UsageHelp;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_cluster_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="cluster a dataset")
public class RemoteClusterDataSetMain implements CheckedConsumer<MarmotRuntime> {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="dataset", index="0", arity="1..1",
				description={"dataset id to cluster"})
	private String m_dsId;
	
	@Option(names="-sample_ratio", paramLabel="ratio", description="sampling ratio (0:1]")
	private double m_sampleRatio;
	
	@Option(names="-fill_ratio", paramLabel="ratio", description="max block-fill ratio")
	private double m_fillRatio;
	
	private long m_blkSize = -1;
	
	@Option(names="-workers", paramLabel="count", description="reduce task count")
	private int m_nworkers = -1;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteClusterDataSetMain cmd = new RemoteClusterDataSetMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
				return;
			}

			PBMarmotClient marmot = cmd.m_connector.connect();
			cmd.accept(marmot);
		}
		catch ( Exception e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		ClusterDataSetOptions options = new ClusterDataSetOptions();
		if ( m_fillRatio > 0 ) {
			options.blockFillRatio(m_fillRatio);
		}
		if ( m_sampleRatio > 0 ) {
			options.sampleRatio(m_sampleRatio);
		}
		if ( m_blkSize > 0 ) {
			options.blockSize(m_blkSize);
		}
		if ( m_nworkers > 0 ) {
			options.workerCount(m_nworkers);
		}
		
		DataSet ds = marmot.getDataSet(m_dsId);
		ds.cluster(options);
	}

	@Option(names="-block_size", paramLabel="nbytes", description="cluster size (eg: '64mb')")
	private void setBlockSize(String blkSizeStr) {
		m_blkSize = UnitUtils.parseByteSize(blkSizeStr);
	}
}
