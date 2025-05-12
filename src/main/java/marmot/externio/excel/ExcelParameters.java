package marmot.externio.excel;

import java.util.List;

import picocli.CommandLine.Option;
import utils.CSV;
import utils.Tuple;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExcelParameters {
	private boolean m_headerFirst = false;
	private FOption<String> m_pointCols = FOption.empty();
	private FOption<String> m_excelSrid = FOption.empty();
	
	public static ExcelParameters create() {
		return new ExcelParameters();
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"-header_first"}, description="consider the first line as header")
	public ExcelParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}

	@Option(names={"-point_cols"}, paramLabel="xy-columns", description="X,Y columns for point")
	public ExcelParameters pointColumns(String pointCols) {
		Utilities.checkNotNullArgument(pointCols, "Point columns are null");
		
		m_pointCols = FOption.ofNullable(pointCols);
		return this;
	}
	
	public FOption<Tuple<String,String>> pointColumns() {
		return m_pointCols.map(cols -> {
			List<String> parts = CSV.parseCsv(cols, ',', '\\').toList();
			return Tuple.of(parts.get(0), parts.get(1));
		});
	}

	@Option(names={"-srid"}, paramLabel="srid", description="EPSG code for input Excel file")
	public ExcelParameters srid(String srid) {
		m_excelSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> srid() {
		return m_excelSrid;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst ? "HF" : "";
		String ptStr = pointColumns().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_excelSrid.map(s -> String.format(", %s", s))
									.getOrElse("");
		return String.format("%s%s%s", headerFirst, ptStr, srcSrid);
	}
}