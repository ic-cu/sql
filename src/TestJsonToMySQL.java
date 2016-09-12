import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.filefilter.WildcardFileFilter;

/*
 * Questa classe dovrebbe leggere un JSON che rappresenta un database e
 * ricrearlo su un database MySQL vivo.
 */
public class TestJsonToMySQL
{

	public static void main(String[] args)
	{
		JsonToMySQL js;
		File dir = new File(args[0]);
		FileFilter ff = new WildcardFileFilter("*.json");
		
		for(File arg : dir.listFiles(ff))
		{
			System.out.println("File: " + arg.getName());
			js = new JsonToMySQL(arg.getName());
			js.createTable();
			js.fillTable();
			System.err.println(js.getSql());
		}
	}
}
