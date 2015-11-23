package de.arvida.warcmock.warc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

public class RecordDB 
{
	
	//================================================================================
    // Constructors
    //================================================================================
	public RecordDB(String archiveFile) throws IOException, ParseException
	{
		// parse WARC archive into m_recordDB
		m_recordDB = new HashMap<URI, SortedList<Record>>();
		readWarc(archiveFile);

		// maintain an iterator pointing to the current record for each URI in m_recordDB
		m_currentRecordsByUri = new HashMap<URI, ListIterator<Record>>();
		for (URI requestUri: m_recordDB.keySet())
		{
			SortedList<Record> list = m_recordDB.get(requestUri);
			ListIterator<Record> iter = list.listIterator();
			m_currentRecordsByUri.put(requestUri, iter);
		}
	}

	
	//================================================================================
    // Public methods
    //================================================================================

	public boolean containsUri(URI requestUri)
	{
		return m_recordDB.containsKey(requestUri);
	}
	
	public Record getNextRecord(URI requestUri)
	{
		ListIterator<Record> currentRecordIterator = m_currentRecordsByUri.get(requestUri);
		
		// this should never be null!
		if (currentRecordIterator != null)
		{	
			Record currentRecord = currentRecordIterator.next();
			if (!currentRecordIterator.hasNext())
			{
				// reset the iterator
				currentRecordIterator =	m_recordDB.get(requestUri).listIterator();
				m_currentRecordsByUri.put(requestUri, currentRecordIterator);
			}
			return currentRecord;
		}
		else
		{
			// this should never be called!
			currentRecordIterator =	m_recordDB.get(requestUri).listIterator();
			m_currentRecordsByUri.put(requestUri, currentRecordIterator);
			return currentRecordIterator.next();
		}
	}
	
	public String getStatistics()
	{
		StringBuilder statistics = new StringBuilder();
	
		statistics.append(String.format("Archive contains %d resource URIs", m_recordDB.size()));
		statistics.append(System.getProperty("line.separator"));
		
		
		for (URI recordUri: m_recordDB.keySet())
		{
			SortedList<Record> recordsByUri = m_recordDB.get(recordUri);
			int recordCountByUri = recordsByUri.size();
			Record oldestRecordByUri = recordsByUri.getFirst();
			Record newestRecordByUri = recordsByUri.getLast();
			
			statistics.append(String.format("For resource URI %s, %d records are stored.", recordUri.toString(), recordsByUri.size()));
			statistics.append(System.getProperty("line.separator"));
			
			statistics.append("Oldest record from ");
			statistics.append(new Date(oldestRecordByUri.getTimestamp()));
			statistics.append(", freshest record from ");
			statistics.append(new Date(newestRecordByUri.getTimestamp()));
			statistics.append(System.getProperty("line.separator"));
			
			long frequency = (newestRecordByUri.getTimestamp() - oldestRecordByUri.getTimestamp()) / recordCountByUri;
			statistics.append(String.format("Assuming update rate of %d ms.", frequency));
			statistics.append(System.getProperty("line.separator"));
			statistics.append(System.getProperty("line.separator"));
		}
		
		return statistics.toString();
	}

	//================================================================================
    // Private methods
    //================================================================================

	/***
	 * Reads WARC archive from given archiveFile.
	 * @param archiveFile
	 * @throws IOException
	 * @throws ParseException 
	 */
	private void readWarc(String archiveFile) throws IOException, ParseException 
	{
		InputStream in = new GZIPInputStream(new FileInputStream(archiveFile));
		WarcReader warcReader = WarcReaderFactory.getReaderUncompressed(in);
		WarcRecord record = null;
		
		// scan each WARC record until we find 
		while ((record = warcReader.getNextRecord()) != null) 
		{ 
			HeaderLine type = record.getHeader("WARC-Type");
			if (type == null || !type.value.equals("response")) 
			{
				continue;
			}

			// a "WARC-Type: response" with valid WARC-Target-URI
			URI requestUri = null;
			HeaderLine uri = record.getHeader("WARC-Target-URI");
			if (uri != null) 
			{
				try 
				{
					requestUri = new URI(uri.value);
				} 
				catch (URISyntaxException e) 
				{
					e.printStackTrace();
					continue;
				}
			} 
			else 
			{
				continue;
			}

			// then create a comparable Record object from WarcRecord and add to m_recordDB
			Record responseRecord = new Record(record);
			insertRecordByUri(requestUri, responseRecord);
		}
		
		warcReader.close();
	}
	
	public synchronized void insertRecordByUri(URI requestUri, Record record) 
	{
	    SortedList<Record> recordsByUri = m_recordDB.get(requestUri);

	    // if list does not exist create it
	    if (recordsByUri == null) 
	    	recordsByUri = new SortedList<Record>(Record.RecordComparator);
	    
	    // add if item is not already in list
	    if(!recordsByUri.contains(record))
	        	recordsByUri.add(record);
	    
	    m_recordDB.put(requestUri, recordsByUri);
	}
	
	//================================================================================
    // Private members
    //================================================================================
	
	private Map<URI, ListIterator<Record>> m_currentRecordsByUri;
	private Map<URI, SortedList<Record>> m_recordDB;
}
