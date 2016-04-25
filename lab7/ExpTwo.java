package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import java.util.Iterator;


public class ExpTwo extends EvalFunc<Float>
{

	private int start;
	private int end;
	
	public ExpTwo(String start, String end)
	{
		this.start = Integer.parseInt(start);
		this.end = Integer.parseInt(end);
	}

	public ExpTwo()
	{

	}
	 
	public Float exec(Tuple input) throws IOException 
	{
		Object o = input.get(0);
		DataBag bag = (DataBag) o;
		Tuple day = null;
		Tuple first_day = null;
		Tuple last_day = null;
		int first = 20200000;
		int last = 18000000;
		Iterator<Tuple> it = bag.iterator();
		while( it.hasNext() )
		{
			day = it.next();
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
		return ((Float)last_day.get(2) / (Float)first_day.get(2));
		
	}
}
