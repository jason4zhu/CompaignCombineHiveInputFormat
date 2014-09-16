package com.judking.hive.inputformat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.NumberUtils;
import org.apache.hadoop.fs.Path;

public class SplitInfo implements Comparable<SplitInfo> {
	Path file = null;
	long start = 0;
	long length = 0;
	String sliceid = "";

	public SplitInfo(Path file, long start, long length) {
		super();
		this.file = file;
		this.start = start;
		this.length = length;

		String s = file.toUri().toString();
		sliceid = String.valueOf(Integer.parseInt(s.substring(s.lastIndexOf('-')+1), 10));
	}

	@SuppressWarnings("deprecation")
	public int compareTo(SplitInfo o) {
		String filestr = this.file.toUri().toString();
		String filestr2 = o.file.toUri().toString();
		int cmp = filestr.compareTo(filestr2);
		if(cmp != 0)
			return cmp;
		else	{
			return NumberUtils.compare(this.start, o.start);
		}
	}
	
	/**
	 * 将多Path、每个Path文件下多个split合成一个完整的start和offset.
	 * e.g.:
	 * Path					start		offset
	 * 20140901/part-0		2			2
	 * 20140902/part-0		3			3
	 * 20140901/part-0		4			2
	 * 20140902/part-0		0			3
	 * 20140901/part-0		0			2
	 * 合并后：
	 * Path					start		offset
	 * 20140901/part-0		0			6
	 * 20140902/part-0		0			6
	 * 
	 * @param sis
	 * @return
	 */
	public static List<SplitInfo> mergeSplitFiles(List<SplitInfo> sis)	{
		if(CollectionUtils.isEmpty(sis))		{
			return new ArrayList<SplitInfo>();
		}
		
		Collections.sort(sis);
		Map<String, List<SplitInfo>> pathMapping = new HashMap<String, List<SplitInfo>>();
		for(SplitInfo si : sis)	{
			String filestr = si.getFile().toUri().toString();
			List<SplitInfo> tmp = pathMapping.get(filestr);
			if(tmp == null)	{
				tmp = new ArrayList<SplitInfo>();
			}
			tmp.add(si);
			pathMapping.put(filestr, tmp);
		}
		
		List<SplitInfo> rtn = new ArrayList<SplitInfo>();
		for(Entry<String, List<SplitInfo>> entry : pathMapping.entrySet())	{
			List<SplitInfo> tmp = entry.getValue();
			Path file= tmp.get(0).getFile();
			long start = tmp.get(0).getStart();
			long offset = tmp.get(tmp.size()-1).getStart()+tmp.get(tmp.size()-1).getLength();
			SplitInfo merged = new SplitInfo(file, start, offset);
			rtn.add(merged);
		}
		
		return rtn;
	}
	
	public Path getFile() {
		return file;
	}

	public void setFile(Path file) {
		this.file = file;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public String getSliceid() {
		return sliceid;
	}

	public void setSliceid(String sliceid) {
		this.sliceid = sliceid;
	}

	@Override
	public String toString() {
		return "SplitInfo [file=" + file + ", start=" + start + ", length="
				+ length + ", sliceid=" + sliceid + "]";
	}

	public static void main(String[] args)		{
		List<SplitInfo> sis = new ArrayList<SplitInfo>();
		sis.add(new SplitInfo(new Path("20140901/part-0"), 0, 2));
		sis.add(new SplitInfo(new Path("20140902/part-0"), 4, 2));
		sis.add(new SplitInfo(new Path("20140901/part-0"), 4, 4));
		sis.add(new SplitInfo(new Path("20140902/part-0"), 0, 2));
		sis.add(new SplitInfo(new Path("20140901/part-0"), 2, 2));
		sis.add(new SplitInfo(new Path("20140902/part-0"), 2, 2));
		System.out.println(SplitInfo.mergeSplitFiles(sis));
	}

}
