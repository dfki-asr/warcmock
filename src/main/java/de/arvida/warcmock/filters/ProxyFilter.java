package de.arvida.warcmock.filters ;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.XMLConfiguration;
import org.jwat.common.HeaderLine;

import de.arvida.warcmock.warc.Record;
import de.arvida.warcmock.warc.RecordDB;

/**
 * @author resc28
 *
 */

@WebFilter(dispatcherTypes = {DispatcherType.REQUEST}, urlPatterns={"*"})
public class ProxyFilter implements Filter 
{
	/***
	 * Applies filter to each incoming request.
	 */
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException 
	{		
		try
		{
			URI requestUri = new URI(((HttpServletRequest)request).getRequestURL().toString());
			
			if (requestUri.equals(m_statisticsURI))
			{
				// handled by StatisticsService
			}
			else if (m_recordDB.containsUri(requestUri))
			{
				// TODO: should be handled by WarcService?
				getFromWARC(requestUri, response);
			}
			else
			{
				// no cache hit -> 404
				((HttpServletResponse) response).setStatus(404);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		chain.doFilter(request, response);
	}

	/***
	 * Sets up filter from config.properties
	 */
	public void init(FilterConfig filterConfig) throws ServletException
	{
		try 
		{
			// parse WARC archive into m_recordDB
			m_configuration = new XMLConfiguration("config.properties");
			m_archivePath = (String) m_configuration.getProperty("warc.path");
			m_recordDB = new RecordDB(m_archivePath);
			m_statisticsURI = new URI((String) m_configuration.getProperty("statistics.uri"));
			
			// place m_recordDB into ServletContext for usage by other servlets
			ServletContext ctx = filterConfig.getServletContext();
			ctx.setAttribute(RECORD_DB, m_recordDB);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	/***
	 * Destroys filter
	 */
	public void destroy() 
	{
		m_configuration = null ;
		m_archivePath = null ;
		m_statisticsURI = null ;
		m_recordDB = null;
		// TODO: remove RECORD_DB from ServletContext
	}
	
	//================================================================================
    // Private methods
    //================================================================================
	
	/***
	 * Fetches a matching response from WARC archive. 
	 * @param requestUri
	 * @param response
	 * @throws IOException
	 */
	private void getFromWARC(URI requestUri, ServletResponse response) throws IOException
	{
		Record currentRecord = m_recordDB.getNextRecord(requestUri);
		
		((HttpServletResponse) response).setStatus(currentRecord.getStatus());
		
		for (HeaderLine hl : currentRecord.getHeaders())  
		{
			((HttpServletResponse) response).setHeader(hl.name, hl.value);
		}
		
		// set WARC timestamp
		((HttpServletResponse) response).setHeader("X-Warc-Timestamp", Long.toString(currentRecord.getTimestamp()));
		((HttpServletResponse) response).setHeader("X-Warc-Original-Timestamp", currentRecord.getWarcTimestamp());
		((HttpServletResponse) response).setHeader("X-Warc-Time", new Date(currentRecord.getTimestamp()).toString());
		
		OutputStream os = response.getOutputStream();
		byte[] ba = currentRecord.getContent();
		ByteArrayInputStream buffer = new ByteArrayInputStream(ba);
		GZIPInputStream gzip = new GZIPInputStream(buffer);

		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = gzip.read(data, 0, data.length)) != -1) 
		{
			os.write(data, 0, nRead);
		}
		
		gzip.close();
		os.flush();
		os.close();
	}

	
	//================================================================================
    // Public members
    //================================================================================
	
	public static final String RECORD_DB = "RECORD_DB";

	//================================================================================
    // Private members
    //================================================================================
	
	private XMLConfiguration m_configuration ;
	private String m_archivePath ;
	private URI m_statisticsURI;
	
	private RecordDB m_recordDB;
}