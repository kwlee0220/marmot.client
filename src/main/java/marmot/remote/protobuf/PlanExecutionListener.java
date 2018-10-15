package marmot.remote.protobuf;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PlanExecutionListener {
	public void notifyStarted();
	
	public void notifyCompleted();
	public void notifyFailed(Throwable failure);
	public void notifyCancelled();
}
