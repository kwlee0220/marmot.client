package marmot.externio.excel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import io.vavr.Tuple2;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;
import utils.Throwables;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportExcel extends ImportIntoDataSet {
	protected final ExcelParameters m_excelParams;
	
	protected abstract FOption<Plan> loadMetaPlan();
	
	public static ImportExcel from(File file, ExcelParameters csvParams,
									ImportParameters importParams) {
		return new ImportExcelFileIntoDataSet(file, csvParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, ExcelParameters csvParams,
									ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, FOption.empty(), csvParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, Plan plan,
											ExcelParameters excelParams,
											ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, FOption.of(plan), excelParams,
												importParams);
	}

	private ImportExcel(ExcelParameters excelParams, ImportParameters importParams) {
		super(importParams);
		
		m_excelParams = excelParams;
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			FOption<Plan> importPlan = loadMetaPlan();
			FOption<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isAbsent() && toPointPlan.isAbsent() ) {
				return FOption.empty();
			}
			if ( importPlan.isAbsent() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isAbsent() ) {
				return importPlan;
			}
			
			return FOption.of(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private FOption<Plan> getToPointPlan() {
		if ( !m_excelParams.pointColumn().isPresent()
			|| !m_params.getGeometryColumnInfo().isPresent() ) {
			return FOption.empty();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple2<String,String> ptCols = m_excelParams.pointColumn().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_excelParams.excelSrid().isPresent() ) {
			String srcSrid = m_excelParams.excelSrid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid, info.srid());
			}
		}
		
		return FOption.of(builder.build());
	}
	
	private static class ImportExcelFileIntoDataSet extends ImportExcel {
		private final File m_start;
		
		ImportExcelFileIntoDataSet(File file, ExcelParameters csvParams,
									ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			return new MultiFileExcelRecordSet(m_start, m_excelParams);
		}

		@Override
		protected FOption<Plan> loadMetaPlan() {
			try {
				return MetaPlanLoader.load(m_start);
			}
			catch ( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static class ImportExcelStreamIntoDataSet extends ImportExcel {
		private final InputStream m_is;
		private final FOption<Plan> m_plan;
		
		ImportExcelStreamIntoDataSet(InputStream is, FOption<Plan> plan,
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
		protected FOption<Plan> loadMetaPlan() {
			return m_plan;
		}
	}
}
