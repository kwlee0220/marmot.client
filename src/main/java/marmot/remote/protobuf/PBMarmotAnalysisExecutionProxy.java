package marmot.remote.protobuf;

import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysisExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotAnalysisExecutionProxy extends PBMarmotExecutionProxy
											implements MarmotAnalysisExecution {
	private final MarmotAnalysis m_analysis;
	
	public PBMarmotAnalysisExecutionProxy(PBPlanExecutionServiceProxy service, MarmotAnalysis analysis,
											String execId) {
		super(service, execId);
		
		m_analysis = analysis;
	}

	@Override
	public MarmotAnalysis getMarmotAnalysis() {
		return m_analysis;
	}

	@Override
	public int getCurrentExecutionIndex() {
		getState();
		
		return m_info.getCurrentExecIndex();
	}
}
