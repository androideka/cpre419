package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import java.util.Iterator;


public class ExpOne extends EvalFunc<Float>
{

	private int start;
	private int end;
	
	public ExpOne(String start, String end)
	{
		this.start = Integer.parseInt(start);
		this.end = Integer.parseInt(end);
	}

	public ExpOne()
	{

	}
	 
	public Float exec(Tuple input) throws IOException 
	{
		Object o = input.get(0);
		DataBag bag = (DataBag) o;
		Tuple day;
		Tuple first_day;
		Tuple last_day;
		int first = 20200000;
		int last = 18000000;
		if( bag != null )
		{
			Iterator<Tuple> it = bag.iterator();
			while( it.hasNext() )
			{
				day = it.next();
				if( day != null )
				{
					int date = (Integer) day.get(1);
					if( date < first && date >= start )
					{
						first = date;
						first_day = day;
					}
					if( date > last && date <= end )
					{
						last = date;
						last_day = day;
					}
					if( first == start && last == end )
					{
						break;
					}
				}
			}
		}
		return (Float)((Float)last_day.get(2) / (Float)first_day.get(2));
		
	}
}
