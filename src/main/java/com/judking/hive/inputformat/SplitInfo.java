package com.judking.hive.inputformat;

import org.apache.hadoop.fs.Path;

public class SplitInfo {
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

}
