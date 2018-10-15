package marmot.externio.excel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.externio.ImportPlanSupplier;
import utils.CommandLine;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportExcel extends ImportIntoDataSet {
	protected final ExcelParameters m_excelParams;
	
	protected abstract Option<Plan> loadMetaPlan();
	
	public static ImportExcel from(File file, ExcelParameters csvParams,
											ImportParameters importParams) {
		return new ImportCsvFileIntoDataSet(file, csvParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, ExcelParameters csvParams,
											ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, Option.none(), csvParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, Plan plan,
											ExcelParameters excelParams,
											ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, Option.some(plan), excelParams,
												importParams);
	}

	private ImportExcel(ExcelParameters excelParams, ImportParameters importParams) {
		super(importParams);
		
		m_excelParams = excelParams;
	}

	@Override
	protected Option<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Option<Plan> importPlan = loadMetaPlan();
			Option<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isEmpty() && toPointPlan.isEmpty() ) {
				return Option.none();
			}
			if ( importPlan.isEmpty() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isEmpty() ) {
				return importPlan;
			}
			
			return Option.some(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private Option<Plan> getToPointPlan() {
		if ( !m_excelParams.pointColumn().isDefined()
			|| !m_params.getGeometryColumnInfo().isDefined() ) {
			return Option.none();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple2<String,String> ptCols = m_excelParams.pointColumn().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_excelParams.excelSrid().isDefined() ) {
			String srcSrid = m_excelParams.excelSrid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid,
												info.srid(), info.name());
			}
		}
		
		return Option.some(builder.build());
	}
	
	private static class ImportCsvFileIntoDataSet extends ImportExcel {
		private final File m_start;
		
		ImportCsvFileIntoDataSet(File file, ExcelParameters csvParams,
								ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			return new MultiFileExcelRecordSet(m_start, m_excelParams);
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
			return ImportPlanSupplier.from(m_start).get();
		}
	}
	
	private static class ImportExcelStreamIntoDataSet extends ImportExcel {
		private final InputStream m_is;
		private final Option<Plan> m_plan;
		
		ImportExcelStreamIntoDataSet(InputStream is, Option<Plan> plan,
									ExcelParameters csvParams, ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_is = is;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				return ExcelRecordSet.from(m_is, m_excelParams);
			}
			catch ( Exception e ) {
				throw new RecordSetException("fails to load ExcelRecordSet: cause=" + e);
			}
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
			return m_plan;
		}
	}

	public static final void runCommand(MarmotRuntime marmot, CommandLine cl) throws IOException {
		File file = new File(cl.getArgument("excel_file"));
		
		boolean headerFirst = cl.hasOption("header_first");
		Option<String> nullString = cl.getOptionString("null_string");
		Option<String> pointCols = cl.getOptionString("point_col");
		Option<String> excelSrid = cl.getOptionString("excel_srid");
		String dsId = cl.getString("dataset");
		String geomCol = cl.getOptionString("geom_col").getOrNull();
		String srid = cl.getOptionString("srid").getOrNull();
		long blkSize = cl.getOptionString("block_size")
						.map(UnitUtils::parseByteSize)
						.getOrElse(-1L);
		boolean force = cl.hasOption("f");
		boolean append = cl.hasOption("a");
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		
		StopWatch watch = StopWatch.start();
		
		ImportParameters importParams = ImportParameters.create()
														.setDatasetId(dsId)
														.setBlockSize(blkSize)
														.setReportInterval(reportInterval)
														.setForce(force)
														.setAppend(append);
		if ( geomCol != null && srid != null ) {
			importParams.setGeometryColumnInfo(geomCol, srid);
		}
		
		ExcelParameters csvParams = ExcelParameters.create()
												.headerFirst(headerFirst);
		pointCols.forEach(csvParams::pointColumn);
		nullString.forEach(csvParams::nullString);
		excelSrid.forEach(csvParams::excelSrid);
		
		ImportIntoDataSet importFile = ImportExcel.from(file, csvParams, importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							dsId, count, watch.getElapsedMillisString(), velo);
	}
}
