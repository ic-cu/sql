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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import sql.DB;

/*
 * Classe per la serializzazione di un DB in JSON.
 */
public class DBToJson
{
	private static Logger log;
	private Properties config;
	private DB source;
	private String sourceSchema;
	private Gson gson;

/*
 * Costruttore che legge il prop file. Le proprietà più importanti sono
 * ovviamente quelle che permettono di accedere correttamente al DB
 */

	public DBToJson(String cFile)
	{
		config = new Properties();
		try
		{
			config.load(new FileReader(cFile));
			initLogger();
			log.info("Connessione al DB...");
			String driver, url, user, pass;
			driver = config.getProperty("source.driver");
			url = config.getProperty("source.url");
			user = config.getProperty("source.username");
			pass = config.getProperty("source.password");
			source = new DB(driver, url, user, pass);
			sourceSchema = config.getProperty("source.schema");
			gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
		}
		catch(FileNotFoundException e)
		{
			log.error("File non trovato: " + e.getMessage());
		}
		catch(IOException e)
		{
			log.error("Impossibile leggere il file di configurazione: " + e.getMessage());
		}
		catch(Exception e)
		{
			log.error("Errore generico: " + e.getMessage());
		}
	}

/*
 * Inizializzazione del logger. I parametri sono nel file di prop.
 */

	private void initLogger()
	{
		PatternLayout pl;
		PrintWriter pw = null;
		WriterAppender wa;
		log = Logger.getLogger("DB2JSON");
		log.setLevel(Level.DEBUG);
		pl = new PatternLayout(config.getProperty("log.pattern"));
		try
		{
			pw = new PrintWriter(config.getProperty("log.file"));
		}
		catch(FileNotFoundException e)
		{
			System.err.println("File di log " + " non trovato: " + e.getMessage());
		}
		wa = new WriterAppender(pl, pw);
		log.addAppender(wa);
		wa = new WriterAppender(pl, System.out);
		log.addAppender(wa);
	}

/*
 * Estrae dal DB tutte le tabelle. L'eventuale schema deve essere specificato
 * nel prop file. L'output è un ResultSet.
 */
	public ResultSet getTables()
	{
		DatabaseMetaData metadata = null;
		ResultSet rs = null;
		try
		{
			Connection conn = source.conn;
			metadata = conn.getMetaData();
			rs = metadata.getTables(null, sourceSchema, "%", null);
		}
		catch(SQLException e)
		{
			log.error("Errore SQL generico: " + e.getMessage());
		}
		return rs;
	}

/*
 * Estrae dal DB le colonne della tabella specificata, anche in questo caso
 * sotto forma di un ResultSet.
 */
	public ResultSet getColumns(String table)
	{
		DatabaseMetaData metadata = null;
		ResultSet rs = null;
		try
		{
			Connection conn = source.conn;
			metadata = conn.getMetaData();
			rs = metadata.getColumns(null, sourceSchema, table, "%");
		}
		catch(SQLException e)
		{
			log.error("Errore SQL generico: " + e.getMessage());
		}
		return rs;
	}

/*
 * Estrae dal DB tutti i record di una tabella. Di questi saranno salvati in
 * JSON i soli valori: i tipi sarnno salvati solo una volta, per cercare di
 * contenere le dimensioni dei file risultati.
 */
	public ResultSet getRecords(String table)
	{
		ResultSet rs = null;
		rs = source.select("select * from " + table);
		return rs;
	}

/*
 * Output del DB in formato JSON. Si comincia con l'elenco di tutte le tabelle,
 * anche se molte saranno scartate perché di sistema. Viene anche creato qualche
 * primo oggetto legato a JSON. Ogni singola tabella sarà salvata nell'oggetto
 * jTable.
 */
	public void output(String outputDir)
	{
		ResultSet rsTables = getTables();
		try
		{
/*
 * Si scorre l'elenco delle tabelle, scartando quelle che iniziano con "sys",
 * perché lo scopo della classe è soprattutto il salvataggio dei dati, non tanto
 * la replica esatta di un DB con relazioni, vincoli etc...
 */
			while(rsTables.next())
			{
				String table = rsTables.getString("TABLE_NAME");
				if(table.startsWith("sys")) continue;
/*
 * Ogni tabella è un oggetto jTable che conterrà i nomi delle colonne e poi
 * tutti i record. C'è anche qualche valore di controllo, tipo il numero di
 * colonne e soprattutto il numero di record. I vari oggetti verranno assemblati
 * insieme solo alla fine, quando il numero di record sarà noto. La prima
 * proprietà della tabella è ovviamente "name".
 */
				JsonObject jTable = new JsonObject();
				jTable.add("name", new JsonPrimitive(table));
				JsonObject jColumns = new JsonObject();
/*
 * I nomi delle colonne si ricavano dal result set dei record, invece che dai
 * metadati del database, quindi in questa fase.
 */
				ResultSet rsRecords = getRecords(table);
				ResultSetMetaData rsmd = rsRecords.getMetaData();
				int rowNum = 0;
/*
 * Si può già salvare una proprietà "numero di colonne" della tabella attuale
 */
				int colNum = rsmd.getColumnCount();
				jTable.add("numberOfColumns", new JsonPrimitive(colNum));
				log.info("Tabella: " + table + " (" + colNum + " colonne)");
/*
 * Si crea l'elenco delle colonne, che si aggiungeranno a jTable come
 * JsonObject, cioè coppie chiave-valore, compresa la lunghezza-precisione.
 */
				for(int i = 1; i <= colNum; i++)
				{
					String column = rsmd.getColumnName(i);
					String cType = rsmd.getColumnTypeName(i);
					int cSize = rsmd.getPrecision(i);
					jColumns.add(column, new JsonPrimitive(cType + "(" + cSize + ")"));
					log.debug("Colonna: " + table + "." + column + " " + cType + "(" + cSize + ")");
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
// jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									jp = new JsonPrimitive((rsRecords.getString(i)));
									break;
								case Types.TIME:
// jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									jp = new JsonPrimitive((rsRecords.getString(i)));
									break;
								case Types.TIMESTAMP:
// jp = new JsonPrimitive((rsRecords.getString(i)).replace(" 00:00:00.0", ""));
									jp = new JsonPrimitive((rsRecords.getString(i)));
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
 * Finito di iterare i record, è bene chiudere il relativo ResultSet, che può
 * essere molto ingombrante, rilasciando le risorse
 */
				rsRecords.close();
/*
 * Ora tutte la parti separate sono pronte e si possono aggiungere a jTable.
 * Prima il numero di record, ormai noto, poi l'oggetto "columns" con i nomi e i
 * tipi di colonne, e infine l'array dei record, con i soli valori. L'array, una
 * volta aggiunto a jTable, si può rilasciare.
 */
				jTable.add("numberOfRecords", new JsonPrimitive(rowNum));
				jTable.add("columns", jColumns);
				jTable.add("records", jRecords);
				jRecords = null;
/*
 * Si scarica jTable sull'apposito file e si rilascia.
 */
				PrintWriter pw = new PrintWriter(new File(outputDir + table + ".json"));
				pw.println(gson.toJson(jTable));
				pw.close();
				jTable = null;
			}
			rsTables.close();
		}
		catch(SQLException e)
		{
			log.error("Errore SQL generico: " + e.getMessage());
		}
		catch(FileNotFoundException e)
		{
			log.error("File non trovato: " + e.getMessage());
		}
	}
}
