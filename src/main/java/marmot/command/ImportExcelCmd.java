package marmot.command;

import java.io.File;

import marmot.MarmotRuntime;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.excel.ExcelParameters;
import marmot.externio.excel.ImportExcel;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.PicocliSubCommand;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="excel", description="import Excel file into the dataset")
public class ImportExcelCmd extends PicocliSubCommand<MarmotRuntime> {
	@Mixin private ExcelParameters m_excelParams;
	@Mixin private ImportParameters m_params;

	@Parameters(paramLabel="path", index="0", arity="1..1",
				description={"path to the excel file to import"})
	private String m_path;
	
	@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
			description={"dataset id to import onto"})
	public void setDataSetId(String id) {
		Utilities.checkNotNullArgument(id, "dataset id is null");
		m_params.setDataSetId(id);
	}

	@Override
	public void run(MarmotRuntime initialContext) throws Exception {
		StopWatch watch = StopWatch.start();
		
		File file = new File(m_path);
		ImportIntoDataSet importFile = ImportExcel.from(file, m_excelParams, m_params);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(initialContext);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							m_params.getDataSetId(), count,
							watch.getElapsedMillisString(), velo);
	}
}
