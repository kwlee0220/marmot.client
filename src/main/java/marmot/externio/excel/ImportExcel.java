package marmot.externio.excel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import utils.Throwables;
import utils.Tuple;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.dataset.GeometryColumnInfo;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportExcel extends ImportIntoDataSet {
	protected final ExcelParameters m_excelParams;
	
	protected abstract Optional<Plan> loadMetaPlan();
	
	public static ImportExcel from(File file, ExcelParameters excelParams,
									ImportParameters importParams) {
		return new ImportExcelFileIntoDataSet(file, excelParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, ExcelParameters csvParams,
									ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, Optional.empty(), csvParams, importParams);
	}
	
	public static ImportExcel from(InputStream is, Plan plan,
											ExcelParameters excelParams,
											ImportParameters importParams) {
		return new ImportExcelStreamIntoDataSet(is, Optional.of(plan), excelParams,
												importParams);
	}

	private ImportExcel(ExcelParameters excelParams, ImportParameters importParams) {
		super(importParams);
		
		m_excelParams = excelParams;
	}

	@Override
	protected Optional<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Optional<Plan> importPlan = loadMetaPlan();
			Optional<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isEmpty() && toPointPlan.isEmpty() ) {
				return Optional.empty();
			}
			if ( importPlan.isEmpty() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isEmpty() ) {
				return importPlan;
			}
			
			return Optional.of(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private Optional<Plan> getToPointPlan() {
		if ( !m_excelParams.pointColumns().isPresent()
			|| !m_params.getGeometryColumnInfo().isPresent() ) {
			return Optional.empty();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple<String,String> ptCols = m_excelParams.pointColumns().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_excelParams.srid().isPresent() ) {
			String srcSrid = m_excelParams.srid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid, info.srid());
			}
		}
		
		return Optional.of(builder.build());
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
		protected Optional<Plan> loadMetaPlan() {
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
		private final Optional<Plan> m_plan;
		
		ImportExcelStreamIntoDataSet(InputStream is, Optional<Plan> plan,
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
		protected Optional<Plan> loadMetaPlan() {
			return m_plan;
		}
	}
}
