import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

import sql.DB;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;

public class WTimeToJson
{
	private static Logger log;
	private Properties config;
	private String dateA;
	private String dateB;
	private String minSep;
	private DB source;

	private void initLogger()
	{
		PatternLayout pl;
		PrintWriter pw = null;
		WriterAppender wa;
		log = Logger.getLogger("WTIME");
		log.setLevel(Level.DEBUG);
		pl = new PatternLayout(config.getProperty("log.pattern"));
		try
		{
			pw = new PrintWriter(config.getProperty("log.file"));
		}
		catch(FileNotFoundException e)
		{
			log.error("File di log " + " non trovato: " + e.getMessage());
		}
		wa = new WriterAppender(pl, pw);
		log.addAppender(wa);
		wa = new WriterAppender(pl, System.out);
		log.addAppender(wa);
	}

	public WTimeToJson()
	{
		config = new Properties();
		try
		{
			config.load(new FileReader("wtime.prop"));
			initLogger();
			log.debug("Inizializzo wtime...");
			minSep = config.getProperty("ods.minsep");
			log.debug("Separatore minuti: '" + minSep + "'");
			String driver, url, user, pass;
			driver = config.getProperty("source.driver");
			url = config.getProperty("source.url");
			user = config.getProperty("source.username");
			pass = config.getProperty("source.password");
			source = new DB(driver, url, user, pass);
		}
		catch(FileNotFoundException e)
		{
			log.warn("File non trovato: " + e.getMessage());
		}
		catch(IOException e)
		{
			log.error("Impossibile leggere il file di configurazione: " + e.getMessage());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		dateA = config.getProperty("date.A");
		dateB = config.getProperty("date.B");
		log.info("Sarà lavorato il periodo dal " + dateA + " al " + dateB);
	}

	public ResultSet getSourceTables()
	{
		DatabaseMetaData metadata = null;
		ResultSet rs = null;
		try
		{
			Connection conn = source.conn;
			metadata = conn.getMetaData();
			rs = metadata.getTables(null, "dbo", "%", null);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		return rs;
	}

	public ResultSet getSourceColumns(String table)
	{
		DatabaseMetaData metadata = null;
		ResultSet rs = null;
		try
		{
			Connection conn = source.conn;
			metadata = conn.getMetaData();
			rs = metadata.getColumns(null, "dbo", table, "%");
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		return rs;
	}

	public ResultSet getSourceRecords(String table)
	{
		ResultSet rs = null;
		rs = source.select("select * from " + table);
		return rs;
	}

	public static void main(String[] args)
	{
		WTimeToJson ts = new WTimeToJson();
/*
 * Si comincia con l'elenco di tutte le tabelle, anche se molte saranno scartate
 * perché di sistema. Viene anche creato qualche primo oggetto legato a JSON. In
 * particolare, gRecord e jTables sono i due oggetti di più alto livello.
 */
		ResultSet rsTables = ts.getSourceTables();
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
		JsonArray jTables = new JsonArray();
/*
 * Il writer serve a scaricare la memoria per evitare heap overflow
 */
		try
		{
			PrintWriter wtime = new PrintWriter(new File("wtime.json"));
			JsonWriter jWriter = new JsonWriter(new PrintWriter(new File("wtime.json")));
			jWriter.beginObject();
/*
 * Si scorre l'elenco delle tabelle, scartando quelle che iniziano con "sys".
 * Prima si inizializza l'output complessivo.
 */
			wtime.println("{\n  \"tables\": [");
			while(rsTables.next())
			{
				String table = rsTables.getString("TABLE_NAME");
				if(table.startsWith("sys")) continue;
				System.err.println(table);
/*
 * Ogni tabella è un oggetto jTable che conterrà i nomi delle colonne e poi
 * tutti i record. Ci vorrebbe anche qualche valore di controllo, tipo il numero
 * di colonne e soprattutto il numero di record
 */
				JsonObject jTable = new JsonObject();
				jTable.add("name", new JsonPrimitive(table));
				JsonObject jColumns = new JsonObject();
/*
 * I nomi delle colonne si ricaveranno dal result set dei record, invece che dai
 * metadati del database
 */
				ResultSet rsRecords = ts.getSourceRecords(table);
				ResultSetMetaData rsmd = rsRecords.getMetaData();
				int colNum = rsmd.getColumnCount();
				int rowNum = 0;
				jTable.add("numberOfColumns", new JsonPrimitive(colNum));
				System.err.println(colNum);
/*
 * Si crea l'elenco delle colonne, che si aggiungeranno a jTable come
 * JsonObiect, cioè coppie chiave-valore, più avanti.
 */
				for(int i = 1; i <= colNum; i++)
				{
					String column = rsmd.getColumnName(i);
					System.err.println(table + "." + column);
					String cType = rsmd.getColumnTypeName(i);
					int cSize = rsmd.getPrecision(i);
					jColumns.add(column, new JsonPrimitive(cType + "(" + cSize + ")"));
				}
/*
 * Si itera sui record, ognuno dei quali è un array di stringhe (jRecord), e
 * insieme saranno un array di oggetti (jRecords). Intanto si calcola il numero
 * di record che si aggiungerà come proprietà a jTable
 */

				JsonArray jRecords = new JsonArray();
				while(rsRecords.next())
				{
					rowNum++;
					JsonArray jRecord = new JsonArray();
					for(int i = 1; i <= colNum; i++)
					{
						JsonPrimitive jp;
						if(rsRecords.getString(i) == null)
						{
							jRecord.add(new JsonPrimitive(""));
						}
						else
						{
							switch(rsmd.getColumnType(i))
							{
								case Types.INTEGER:
									jp = new JsonPrimitive(rsRecords.getLong(i));
									break;
								case Types.SMALLINT:
									jp = new JsonPrimitive(rsRecords.getLong(i));
									break;
								case Types.DATE:
									jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									break;
								case Types.TIME:
									jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									break;
								case Types.TIMESTAMP:
									jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									break;
								case Types.VARCHAR:
									jp = new JsonPrimitive(rsRecords.getString(i));
									break;
								case Types.BOOLEAN:
									jp = new JsonPrimitive(rsRecords.getBoolean(i));
									break;
								default:
									jp = new JsonPrimitive(rsRecords.getString(i));
									break;
							}
							jRecord.add(jp);
						}
					}
					jRecords.add(jRecord);
				}
/*
 * Il result set dei record può essere molto ingombrante, e a questo punto non
 * serve più, per cui è opportuno rilasciarlo. Poi si completa jTable e si
 * aggiunge a jTables. Qui già si può salvare la singola tabella in un apposito
 * file. Anche qualche oggetto JSON può essere annullato una volta copiato in un
 * altro, almeno jRecords che ovviamente è il più corposo. Poi si può annullare
 * anche jTable, che ovviamente contiene copia di jRecords.
 */
				rsRecords.close();
				jTable.add("numberOfRecords", new JsonPrimitive(rowNum));
				jTable.add("columns", jColumns);
				jTable.add("records", jRecords);
				jRecords = null;
				jTables.add(jTable);

				PrintWriter pw = new PrintWriter(new File(table + ".json"));
				String json = gson.toJson(jTable);
				jTable = null;

				pw.println(json);
				pw.close();
				wtime.print(json);
				if(!rsTables.isLast())
				{
					wtime.println(",");
				}
				else
				{
					wtime.println();
				}
				wtime.flush();
			}
/*
 * Finite le tabelle, è opportuno rilasciare anche il relativo result set, anche
 * se non è paragonabile a quello dei record di certe tabelle. Poi si aggiunge
 * jTables a gRecord che si scrive su un file complessivo
 */
			rsTables.close();
// gRecord.add("tables", jTables);
			jTables = null;
			wtime.println("}");
			wtime.close();
			jWriter.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}
