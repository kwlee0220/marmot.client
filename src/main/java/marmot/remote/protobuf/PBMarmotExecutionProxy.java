package marmot.remote.protobuf;

import static marmot.support.DateTimeFunctions.DateTimeFromMillis;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.proto.service.ExecutionInfoProto;
import marmot.proto.service.ExecutionInfoProto.ExecutionStateInfoProto;
import marmot.proto.service.ExecutionInfoProto.ExecutionStateProto;
import marmot.protobuf.PBUtils;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotExecutionProxy implements MarmotExecution {
	private static final long TIME_PRECISION = 1000;	// 1s
	
	private final PBPlanExecutionServiceProxy m_service;
	private final String m_execId;
	private final FOption<MarmotAnalysis> m_analysis;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private long m_ts;
	@GuardedBy("m_guard") private ExecutionInfoProto m_info;
	@GuardedBy("m_guard") private State m_state = State.RUNNING;
	@GuardedBy("m_guard") private Throwable m_cause = null;
	
	public PBMarmotExecutionProxy(PBPlanExecutionServiceProxy service, ExecutionInfoProto info) {
		m_service = service;
		m_execId = info.getId();
		
		switch ( info.getOptionalAnalysisIdCase() ) {
			case ANALYSIS_ID:
				String analId = info.getAnalysisId();
				m_analysis = FOption.of(m_service.getAnalysis(analId));
				break;
			case OPTIONALANALYSISID_NOT_SET:
				m_analysis = FOption.empty();
				break;
			default:
				throw new AssertionError();
		}
		
		update(info);
	}

	@Override
	public String getId() {
		return m_execId;
	}

	@Override
	public FOption<MarmotAnalysis> getMarmotAnalysis() {
		return m_analysis;
	}

	@Override
	public int getCurrentExecutionIndex() {
		if ( m_state == State.RUNNING ) {
			update();
		}
		
		return m_info.getCurrentExecIndex();
	}

	@Override
	public State getState() {
		if ( m_state == State.RUNNING ) {
			update();
		}
		
		return m_state;
	}

	@Override
	public Throwable getFailureCause() throws IllegalStateException {
		if ( m_state == State.RUNNING ) {
			update();
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
			update(m_service.waitForFinished(m_execId));
		}
	}

	@Override
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException {
		if ( m_state == State.RUNNING ) {
			update(m_service.waitForFinished(m_execId, timeout, unit));
		}
		
		return m_state != State.RUNNING;
	}

	@Override
	public long getStartedTime() {
		return m_info.getStartedTime();
	}

	@Override
	public long getFinishedTime() {
		if ( m_state == State.RUNNING ) {
			update();
		}
		
		return m_info.getFinishedTime();
	}

	@Override
	public Duration getMaximumRunningTime() {
		if ( m_state == State.RUNNING ) {
			update();
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
		update();
		
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
		State state = getState();
		
		String analyStr = m_analysis.map(anal -> String.format(", %s[%s]", anal.getType(), anal.getId()))
									.getOrElse("");
		
		String failedCause = "";
		if ( state == State.FAILED ) {
			failedCause = String.format(" (cause=%s)", m_cause);
		}
		
		long elapsed = ( state == State.RUNNING )
					? System.currentTimeMillis() - m_info.getStartedTime()
					: m_info.getFinishedTime() - m_info.getStartedTime();
		String elapsedStr = UnitUtils.toSecondString(elapsed);

		LocalDateTime startedStr = DateTimeFromMillis(m_info.getStartedTime());
		return String.format("%10s: %9s%s%s, started=%s, elapsed=%s", getId(), state,
							failedCause, analyStr, startedStr, elapsedStr);
	}
	
	private void update() {
		if ( (System.currentTimeMillis() - m_guard.get(() -> m_ts)) < TIME_PRECISION ) {
			return;
		}
		
		update(m_service.getExecutionInfo(m_execId));
	}
	
	private void update(ExecutionInfoProto info) {
		m_guard.lock();
		try {
			m_info = info;
			
			m_state =  fromExecutionStateProto(m_info.getExecStateInfo().getState());
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
			
			m_ts = System.currentTimeMillis();
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private static State fromExecutionStateProto(ExecutionStateProto proto) {
		switch ( proto ) {
			case EXEC_RUNNING:
				return State.RUNNING;
			case EXEC_COMPLETED:
				return State.COMPLETED;
			case EXEC_CANCELLED:
				return State.CANCELLED;
			case EXEC_FAILED:
				return State.FAILED;
			default:
				throw new AssertionError();
		}
	}
}
