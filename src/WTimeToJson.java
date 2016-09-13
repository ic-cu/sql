
public class WTimeToJson
{
	public static void main(String[] args)
	{
		DBToJson ts = new DBToJson("wtime.prop");
		ts.output("tmp/");
	}
}
