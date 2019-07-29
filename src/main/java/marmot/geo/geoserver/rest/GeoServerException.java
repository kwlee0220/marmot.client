package marmot.geo.geoserver.rest;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoServerException extends MarmotRuntimeException {
	private static final long serialVersionUID = 2279095051087998901L;
	
	public GeoServerException(String details) {
		super(details);
	}
	
	public GeoServerException(Exception cause) {
		super(cause);
	}

}
