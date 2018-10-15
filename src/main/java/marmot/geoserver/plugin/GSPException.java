package marmot.geoserver.plugin;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPException extends RuntimeException {
	private static final long serialVersionUID = -5673595374631816189L;

	public GSPException(String details) {
		super(details);
	}
}
