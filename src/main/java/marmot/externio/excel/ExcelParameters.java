package marmot.externio.excel;

import java.util.List;
import java.util.Objects;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExcelParameters {
	private boolean m_headerFirst = false;
	private Option<String> m_nullString = Option.none();
	private Option<String> m_pointCols = Option.none();
	private Option<String> m_excelSrid = Option.none();
	
	public static ExcelParameters create() {
		return new ExcelParameters();
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}
	
	public ExcelParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}
	
	public Option<String> nullString() {
		return m_nullString;
	}
	
	public ExcelParameters nullString(String nullString) {
		m_nullString = Option.of(nullString);
		return this;
	}
	
	public ExcelParameters pointColumn(String pointCols) {
		Objects.requireNonNull(pointCols, "Point columns are null");
		
		m_pointCols = Option.of(pointCols);
		return this;
	}
	
	public Option<Tuple2<String,String>> pointColumn() {
		return m_pointCols.map(cols -> {
			List<String> parts = CSV.parse(cols, ',', '\\');
			return Tuple.of(parts.get(0), parts.get(1));
		});
	}
	
	public ExcelParameters excelSrid(String srid) {
		m_excelSrid = Option.of(srid);
		return this;
	}
	
	public Option<String> excelSrid() {
		return m_excelSrid;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst ? ", header" : "";
		String ptStr = pointColumn().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_excelSrid.map(s -> String.format(", csv_srid=%s", s))
									.getOrElse("");
		return String.format("%s%s%s", headerFirst, ptStr, srcSrid);
	}
}