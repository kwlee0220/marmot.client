package marmot.remote.protobuf;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import marmot.exec.MarmotExecution;
import marmot.proto.service.ExecutionInfoProto;
import marmot.proto.service.ExecutionInfoProto.ExecutionStateInfoProto;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotExecutionProxy implements MarmotExecution {
	private final PBPlanExecutionServiceProxy m_service;
	private final String m_execId;
	protected ExecutionInfoProto m_info;
	private State m_state = State.RUNNING;
	private Throwable m_cause = null;
	
	public PBMarmotExecutionProxy(PBPlanExecutionServiceProxy service, String execId) {
		m_service = service;
		m_execId = execId;
	}

	@Override
	public String getId() {
		return m_execId;
	}

	@Override
	public synchronized State getState() {
		if ( m_state == State.RUNNING ) {
			updateExecutionInfo(m_service.getExecutionInfo(m_execId));
		}

		return m_state;
	}

	@Override
	public Throwable getFailureCause() throws IllegalStateException {
		if ( m_state == State.RUNNING ) {
			getState();
		}
		if ( m_state == State.FAILED ) {
			return m_cause;
		}
		
		throw new IllegalStateException("not failed state: state=" + getState());
	}

	@Override
	public boolean cancel() {
		return m_service.cancelExecution(m_execId);
	}

	@Override
	public void waitForFinished() throws InterruptedException {
		if ( m_state == State.RUNNING ) {
			updateExecutionInfo(m_service.waitForFinished(m_execId));
		}
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		if ( m_state == State.RUNNING ) {
			updateExecutionInfo(m_service.waitForFinished(m_execId, timeout, unit));
		}
		
		return m_state != State.RUNNING;
	}

	@Override
	public long getStartedTime() {
		if ( m_info == null ) {
			updateExecutionInfo(m_service.getExecutionInfo(m_execId));
		}
		
		return m_info.getStartedTime();
	}

	@Override
	public long getFinishedTime() {
		if ( m_state == State.RUNNING ) {
			updateExecutionInfo(m_service.getExecutionInfo(m_execId));
		}
		
		return m_info.getFinishedTime();
	}

	@Override
	public Duration getMaximumRunningTime() {
		if ( m_state == State.RUNNING ) {
			updateExecutionInfo(m_service.getExecutionInfo(m_execId));
		}
		
		return Duration.ofMillis(m_info.getMaxRunningTime());
	}

	@Override
	public void setMaximumRunningTime(Duration dur) {
		ExecutionInfoProto info = m_info.toBuilder()
										.setMaxRunningTime(dur.toMillis())
										.build();
		m_service.setExecutionInfo(m_execId, info);
		m_info = info;
	}

	@Override
	public Duration getRetentionTime() {
		updateExecutionInfo(m_service.getExecutionInfo(m_execId));
		
		return Duration.ofMillis(m_info.getRetentionTime());
	}

	@Override
	public void setRetentionTime(Duration dur) {
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
		String causeStr = (m_cause != null) ? m_cause.toString() : "";
		return String.format("%s(%s,%s%s)", getClass().getSimpleName(), m_execId, m_state, causeStr);
	}
	
	protected void updateExecutionInfo(ExecutionInfoProto infoProto) {
		m_info = infoProto;
		m_state =  PBUtils.fromExecutionStateProto(m_info.getExecStateInfo().getState());
		ExecutionStateInfoProto stateProto = m_info.getExecStateInfo();
		switch ( stateProto.getOptionalFailureCauseCase() ) {
			case FAILURE_CAUSE:
				m_cause = PBUtils.toException(stateProto.getFailureCause());
				break;
			case OPTIONALFAILURECAUSE_NOT_SET:
				m_cause = null;
				break;
			default:
				throw new AssertionError();
		}
	}
}
