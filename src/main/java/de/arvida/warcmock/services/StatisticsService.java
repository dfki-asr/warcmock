package de.arvida.warcmock.services;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletContext;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.arvida.warcmock.filters.ProxyFilter;
import de.arvida.warcmock.warc.RecordDB;


@WebServlet(loadOnStartup=1, urlPatterns={"/statistics"})
public class StatisticsService extends HttpServlet 
{
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException 
	{
		getTOC(request, response);
    }

	/***
	 * Generates a table-of-content (TOC) of the WARC archive.
	 * @param requestUri
	 * @param response
	 * @throws IOException
	 */
	private void getTOC(HttpServletRequest request, ServletResponse response) throws IOException
	{
		ServletContext ctx = request.getServletContext();
		RecordDB record_db = (RecordDB) ctx.getAttribute(ProxyFilter.RECORD_DB);
		
		// TODO: this could be described in VOID vocabulary
		response.setContentType("text/plain");
		PrintWriter pw = response.getWriter();
		pw.println(record_db.getStatistics());
		pw.flush();
		pw.close();
	}
	
	private static final long serialVersionUID = 6952155988889808227L;
	

}
