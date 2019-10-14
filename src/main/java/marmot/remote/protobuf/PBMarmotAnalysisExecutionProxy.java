package marmot.remote.protobuf;

import static marmot.support.DateTimeFunctions.DateTimeFromMillis;

import java.time.LocalDateTime;

import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysisExecution;
import marmot.proto.service.ExecutionInfoProto;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotAnalysisExecutionProxy extends PBMarmotExecutionProxy
											implements MarmotAnalysisExecution {
	private MarmotAnalysis m_analysis = null;
	
	public PBMarmotAnalysisExecutionProxy(PBPlanExecutionServiceProxy service,
											ExecutionInfoProto info) {
		super(service, info);
	}
	
	public PBMarmotAnalysisExecutionProxy(PBPlanExecutionServiceProxy service,
											MarmotAnalysis analysis, ExecutionInfoProto info) {
		super(service, info);
		
		m_analysis = analysis;
	}

	@Override
	public MarmotAnalysis getMarmotAnalysis() {
		if ( m_analysis == null ) {
			m_analysis = m_service.getAnalysis(m_info.getAnalysisId());
		}
		
		return m_analysis;
	}

	@Override
	public int getCurrentExecutionIndex() {
		getState();
		
		return m_info.getCurrentExecIndex();
	}
	
	@Override
	public String toString() {
		State state = getState();
		
		String failedCause = "";
		if ( m_state == State.FAILED ) {
			failedCause = String.format(" (cause=%s)", m_cause);
		}
		
		String tailStr = "";
		if ( state != State.RUNNING ) {
			long millis = m_info.getFinishedTime() - m_info.getStartedTime();
			
			tailStr = String.format(", elapsed=%s", UnitUtils.toSecondString(millis));
		}
		else {
			if ( m_info.getFinishedTime() > 0 ) {
				tailStr = String.format(", finished=%s", DateTimeFromMillis(m_info.getFinishedTime()));
			}
		}

		MarmotAnalysis anal = getMarmotAnalysis();
		LocalDateTime ldt = DateTimeFromMillis(m_info.getStartedTime());
		return String.format("%s: %s%s, %s[%s], started=%s%s", getId(), m_state,
							failedCause, anal.getType(), anal.getId(), ldt, tailStr);
	}
}
