/**
 * 
 */
package de.arvida.warcmock.warc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

/**
 * @author resc01
 *
 */
public class Record //implements Comparable<Record>
{
	//================================================================================
    // Comparator
    //================================================================================
	
	public static Comparator<Record> RecordComparator = new Comparator<Record>() 
	{
		public int compare(Record o1, Record o2) 
		{
			return o1.m_warcTimestamp.compareTo(o2.m_warcTimestamp);
		}
	};
	
	//================================================================================
    // Constructors
    //================================================================================
	
	Record(WarcRecord warcRecord) throws IOException, ParseException
	{
		parseWarcRecord(warcRecord);
	}
	
	Record(int timestamp, int status, List<HeaderLine> headers, byte[] content)
	{
		m_timestamp = timestamp;
		m_status = status;
		m_headers = headers;
		m_content = content;
	}

	//================================================================================
    // Public methods
    //================================================================================
	
	public long getTimestamp()
	{
		return m_timestamp;
	}
	
	public String getWarcTimestamp()
	{
		return m_warcTimestamp;
	}
	
	
	public int getStatus()
	{
		return m_status;
	}
	
	public List<HeaderLine> getHeaders()
	{
		return m_headers;
	}
	
	public byte[] getContent()
	{
		return m_content;
	}

	//================================================================================
    // Private methods
    //================================================================================

	private void parseWarcRecord(WarcRecord warcRecord) throws IOException, ParseException
	{
		HttpHeader header = warcRecord.getHttpHeader();
		
		m_status = header.statusCode;
		m_headers = header.getHeaderList();
 
		Payload payload = warcRecord.getPayload();
		InputStream pin = payload.getInputStream();
		m_content = toGzipByteArray(pin);
		pin.close();
		
		// TODO: SimpleDateFormat is broken!
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		m_timestamp = sdf.parse(warcRecord.getHeader("WARC-Timestamp").value).getTime();
		m_warcTimestamp = warcRecord.getHeader("WARC-Timestamp").value;
	}

	private byte[] toGzipByteArray(InputStream is) throws IOException 
	{
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(buffer);
		
		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = is.read(data, 0, data.length)) != -1) 
		{
			gzip.write(data, 0, nRead);
		}

		gzip.flush();
		gzip.close();
		return buffer.toByteArray();
	}

	//================================================================================
    // Private members
    //================================================================================
	private String m_warcTimestamp ;
	private long m_timestamp;
	private int m_status;
	private List<HeaderLine> m_headers;
	private byte[] m_content;
}