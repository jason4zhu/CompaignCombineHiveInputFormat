package com.judking.hive.inputformat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NumberUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.hash.HashFunction;



public class main {
	public static void main(String[] args)	{
		//CombineHiveInputFormat<WritableComparable, Writable> ll;
		
//		Multimap<String, Integer> mm = ArrayListMultimap.create();
//		mm.put("11", 2);
//		mm.put("11", 3);
//		mm.put("11", 2);
//		mm.put("22", 111);
//		for (String key : new HashSet<String>(mm.keys()))	{
//			System.out.println(key);
//			List<Integer> l = new ArrayList<Integer>(mm.get(key));
//			System.out.println(l);
//		}
		
		
		System.out.println(Math.abs("hdfs://ns1/tmp/hive-monitor/hive_2014-10-15_14-08-30_808_4704385148168585552-1/-mr-10002/000018_0".hashCode()) % 5);
		
//		System.out.println(NumberUtils.compare(2L, 2L));
		
//		Path p = new Path("/tong/data/output/dailyMerger/20140901/minisite_part-r-00000");
//		String s = p.toUri().toString();
//		s = String.valueOf(Integer.parseInt(s.substring(s.lastIndexOf('-')+1), 10));
//		System.out.println(s);
		
//		List<A> aa = new ArrayList<A>();
//		aa.add(new A());
//		aa.add(new A());
//		System.out.println(aa);
		
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
