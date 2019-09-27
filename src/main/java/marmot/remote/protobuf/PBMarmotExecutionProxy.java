package marmot.remote.protobuf;

import java.util.concurrent.TimeUnit;

import marmot.MarmotExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotExecutionProxy implements MarmotExecution {
	private final PBPlanExecutionServiceProxy m_service;
	private final String m_execId;
	
	public PBMarmotExecutionProxy(PBPlanExecutionServiceProxy service, String execId) {
		m_service = service;
		m_execId = execId;
	}

	@Override
	public String getId() {
		return m_execId;
	}

	@Override
	public State getState() {
		return m_service.getExecutionState(m_execId);
	}

	@Override
	public boolean cancel() {
		return m_service.cancelExecution(m_execId);
	}

	@Override
	public void waitForFinished() throws InterruptedException {
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !PBMarmotExecutionProxy.class.equals(obj.getClass()) ) {
			return false;
		}
		
		PBMarmotExecutionProxy other = (PBMarmotExecutionProxy)obj;
		return m_execId.equals(other.m_execId);
	}
	
	@Override
	public int hashCode() {
		return m_execId.hashCode();
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + m_execId + "]";
	}
}
