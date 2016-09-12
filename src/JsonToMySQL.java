import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Vector;

import com.google.gson.stream.JsonReader;

/*
 * Questa classe dovrebbe leggere un JSON che rappresenta un database e
 * ricrearlo su un database MySQL vivo.
 */
public class JsonToMySQL
{

	JsonReader reader = null;
	private String sql = null;
	private String inFileName = null;
	String curTableName = null;
	int curTableColumns = 0;
	int curTableRecords = 0;
	private String sep = "	";
	Vector<String> curTableTypes;

/*
 * Costruttore minimale. Apre il file e l'oggetto top level
 */

	public JsonToMySQL(String input)
	{
		try
		{
			reader = new JsonReader(new FileReader(input));
			reader.beginObject();
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
	
/*
 * Siccome il reader è aperto nel costruttore, è il caso di pensare a chiuderlo.	
 */
	
	public void closeReader()
	{
		try
		{
			reader.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

/*
 * Si prelevano dal file JSON i dati relativi alla tabella e si produce una
 * istruzione CREATE opportuna che sarà registrata nel membro "sql", come altre
 * istruzioni più avanti
 */
 	public void createTable()
	{
		String pName;
		String cName, cType;
		curTableTypes = new Vector<String>();
		try
		{
			pName = reader.nextName();
			if(pName.equals("name")) curTableName = reader.nextString().toLowerCase();
			sql = "DROP TABLE " + curTableName + ";\n";
			sql += "CREATE TABLE " + curTableName + "\n(\n";
			inFileName = curTableName + ".txt";
			pName = reader.nextName();
			if(pName.equals("numberOfColumns")) curTableColumns = reader.nextInt();
			pName = reader.nextName();
			if(pName.equals("numberOfRecords")) curTableRecords = reader.nextInt();
			pName = reader.nextName();

// Si scorre l'elenco delle colonne, cercando di mappare i tipi per MySQL

			if(pName.equals("columns"))
			{
				reader.beginObject();
				while(reader.hasNext())
				{
					cName = reader.nextName();
					cType = reader.nextString();
					curTableTypes.add(cType);
					switch(cType)
					{
						case "varchar":
							cType = "VARCHAR(1024)";
							break;

						case "int":
							cType = "INT";
							break;

						case "smallint":
							cType = "INT";
							break;

						case "datetime":
							cType = "DATETIME";
							break;

						default:
							cType = "VARCHAR(1024)";
							break;
					}
					sql += sep + cName + " " + cType;
					if(reader.hasNext())
					{
						sql += ",\n";
					}
				}
				reader.endObject();
			}
			sql += "\n);\n\n";
			sql += "LOAD DATA LOCAL INFILE \'" + inFileName + "\' INTO TABLE " + curTableName + ";\n";
			PrintWriter pw = new PrintWriter(new File(curTableName + ".sql"));
			pw.println(sql);
			pw.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

/*
 * Scandisce il file JSON recuperando i record e producendo le righe nel formato
 * adatto a LOAD DATA. I dati vanno nella stringa "inFileData", ma in "sql" si
 * aggiunge l'opportuna LOAD DATA
 */

	public String fillTable()
	{
		String pName;
		String value = null;
		String record;
		try
		{
			PrintWriter pw = new PrintWriter(new File(inFileName));
			pName = reader.nextName();
			
/*
 * Per controllo, si contano i record effettivi e le colonne di ogni record,
 * confrontandoli con curTableColumns/Records
 */
			
			if(pName.equals("records"))
			{
				reader.beginArray();
				int records = 0;
				while(reader.hasNext())
				{
					reader.beginArray();
					int columns = 0;
					records++;
					record = "";
					while(reader.hasNext())
					{
						value = reader.nextString();
						columns++;
						if(value.isEmpty())
						{
							record += "\\N";
						}
						else
						{
							record += value;
						}
						if(reader.hasNext())
						{
							record += sep; // attenzione, qui c'è un TAB!
						}
					}
					pw.println(record);
					if(columns != curTableColumns)
					{
						System.err.println("*** Trovate " + columns + " invece di " + curTableColumns + "****");
					}
					reader.endArray();
				}
				if(records != curTableRecords)
				{
					System.err.println("Trovate " + records + " invece di " + curTableRecords);
				}
				reader.endArray();
			}
			pw.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public String getSql()
	{
		return sql;
	}

	public void setSql(String sql)
	{
		this.sql = sql;
	}
}
