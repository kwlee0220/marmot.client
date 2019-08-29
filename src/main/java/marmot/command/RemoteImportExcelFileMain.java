package marmot.command;

import java.io.File;

import marmot.externio.ImportIntoDataSet;
import marmot.externio.excel.ExcelParameters;
import marmot.externio.excel.ImportExcel;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_import_excel",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="import Excel files into a dataset")
public class RemoteImportExcelFileMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private ExcelParameters m_excelParams;
	@Mixin private ImportParameters m_params;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="path", index="0", arity="1..1",
				description={"path to the excel file to import"})
	private String m_path;
	
	@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
			description={"dataset id to import onto"})
	public void setDataSetId(String id) {
		Utilities.checkNotNullArgument(id, "dataset id is null");
		m_params.setDataSetId(id);
	}

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteImportExcelFileMain cmd = new RemoteImportExcelFileMain();
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
			
			StopWatch watch = StopWatch.start();
			
			File file = new File(m_path);
			ImportIntoDataSet importFile = ImportExcel.from(file, m_excelParams, m_params);
			importFile.getProgressObservable()
						.subscribe(report -> {
							double velo = report / watch.getElapsedInFloatingSeconds();
							System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
											report, watch.getElapsedMillisString(), velo);
						});
			long count = importFile.run(marmot);
			
			double velo = count / watch.getElapsedInFloatingSeconds();
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
								m_params.getDataSetId(), count,
								watch.getElapsedMillisString(), velo);
		}
		catch ( Exception e ) {
			System.err.println("" + e);
		}
	}
}
