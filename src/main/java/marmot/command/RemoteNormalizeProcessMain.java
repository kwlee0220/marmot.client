package marmot.command;

import marmot.MarmotRuntime;
import marmot.process.NormalizeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.CSV;
import utils.func.CheckedConsumer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_normalize",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="normalize input data column(s)")
public class RemoteNormalizeProcessMain implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	private static class Params {
		@Parameters(paramLabel="in_ds_id", index="0", arity="1..1",
					description={"input dataset id"})
		private String m_inputDsId;

		@Parameters(paramLabel="out_ds_id", index="1", arity="1..1",
					description={"output dataset id"})
		private String m_outputDsId;
		
		private String[] m_inFeatures;

		@Parameters(paramLabel="in_feature_cols", index="2", arity="1..1",
					description={"input feature columns"})
		private void setInputFeatures(String cols) {
			m_inFeatures = CSV.parseCsvAsArray(cols);
		}
		
		private String[] m_outFeatures;

		@Parameters(paramLabel="out_feature_colsls", index="3", arity="1..1",
					description={"output feature columns"})
		private void setOutputFeatures(String cols) {
			m_outFeatures = CSV.parseCsvAsArray(cols);
		}
	}

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteNormalizeProcessMain cmd = new RemoteNormalizeProcessMain();
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
		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(m_params.m_inputDsId);
		params.outputDataset(m_params.m_outputDsId);
		params.inputFeatureColumns(m_params.m_inFeatures);
		params.outputFeatureColumns(m_params.m_outFeatures);
		
		marmot.executeProcess("normalize", params.toMap());
	}
}
