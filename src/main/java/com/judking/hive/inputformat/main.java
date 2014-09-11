package com.judking.hive.inputformat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;



public class main {
	public static void main(String[] args)	{
		//CombineHiveInputFormat<WritableComparable, Writable> ll;
		
//		Path p = new Path("/user/supertool/zhudi/hiveTest/20140902/0902_1.txt");
//		String s = p.toUri().toString();
//		s = s.substring(s.lastIndexOf('_')+1, s.lastIndexOf('.'));
//		System.out.println(s);
		
		List<A> aa = new ArrayList<A>();
		aa.add(new A());
		aa.add(new A());
		System.out.println(aa);
		
//		Map<String, String> m = new HashMap<String, String>();
//		m.put("1", "2");
//		m.put("2", "2");
//		System.out.println(m.size());
		
		
//		Path[] ps = new Path[1];
//		ps[0] = new Path("123");
//		ps = org.apache.commons.lang3.ArrayUtils.add(ps, new Path("456"));
//		List<String> pathUrls = new ArrayList<String>();
//		for(int j = 0; j < ps.length; ++j)	{
//			pathUrls.add(ps[j].toUri().toString());
//		}
//		System.out.println(pathUrls);
		
//		long[] a = new long[]{1,2,3,4,5};
//		a = ArrayUtils.add(a, 66L);
//		System.out.println(""+Arrays.asList(ArrayUtils.toObject(a))+"");
		
//		List<String> a = new ArrayList<String>();
//		System.out.println();
		
//		org.apache.hadoop.hive.ql.io.HiveInputFormat<WritableComparable, Writable>
//		org.apache.hadoop.hive.ql.io.hiveo
//		org.apache.hadoop.hive.ql.io.HiveInputFormat<WritableComparable, Writable>
		
//		int start = 24;
//		int maxx = start+3;
//		for(int i = start; i < maxx; ++i)	{
//			System.out.println(i+"\t"+(int)(Math.random()*99999)+"\t"+"20140903");
//		}
		
	}
}

class A	{
	public int a = 1;

	@Override
	public String toString() {
		return "A [a=" + a + "]";
	}
	
}
