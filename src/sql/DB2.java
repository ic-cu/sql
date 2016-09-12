package sql;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;

public class DB2
{
	private SQLServerDataSource ds;
	private Connection connection;
	private Properties config;

	private static void err(String str)
	{
		System.err.println(str);
	}

	public DB2()
	{
		try
		{
			config = new Properties();
			config.load(new FileReader("db.prop"));
			Class.forName(config.getProperty("database.driver"));
			ds = new SQLServerDataSource();
			ds.setServerName(config.getProperty("database.host"));
			ds.setInstanceName(config.getProperty("database.instancename"));
			ds.setDatabaseName(config.getProperty("database.name"));
			ds.setSelectMethod("direct");
			ds.setUser(config.getProperty("database.username"));
			ds.setPassword(config.getProperty("database.password"));
			connection = ds.getConnection();
			if(connection == null)
			{
				err("Connessione nulla!");
			}
		}
		catch(ClassNotFoundException e)
		{
			err("*** Driver " + config.getProperty("database.driver")
					+ " non trovato ***");
			e.printStackTrace();
		}
		catch(SQLServerException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(FileNotFoundException e)
		{
			err("*** File di configurazione non trovato");
			e.printStackTrace();
		}
		catch(IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection getConnection()
	{
		return connection;
	}

	public ResultSet executeQuery(String query)
	{
		Statement st;
		ResultSet rs = null;
		try
		{
			st = connection.createStatement();
			st.executeQuery(query);
			rs = st.getResultSet();
		}
		catch(SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rs;
	}

	public void close()
	{
		try
		{
			connection.close();
		}
		catch(SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
