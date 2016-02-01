/**
 * Created by androideka on 1/26/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.lang.String;

public class lab1 {

    public static void main(String[] args) throws Exception
    {
        Configuration sys_config = new Configuration();

        FileSystem filesys = FileSystem.get(sys_config);

        Path filepath = new Path("/class/s16419x/lab1/bigdata");

        FSDataInputStream file = filesys.open(filepath);

        byte[] buffer = new byte[1000];
        file.read(5000000000L, buffer, 0, 1000);
        byte checksum = buffer[0];
        for(int i = 1; i < 1000; i++)
        {
            checksum ^= buffer[i];
        }
        System.out.println("Checksum: " + String.format("%8s", Integer.toBinaryString(checksum)).replace(' ', '0'));
    }

}
