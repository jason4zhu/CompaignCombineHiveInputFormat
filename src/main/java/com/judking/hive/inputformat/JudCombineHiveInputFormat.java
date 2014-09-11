package com.judking.hive.inputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.open_txns;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.CombineHiveInputSplit;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class JudCombineHiveInputFormat<K extends WritableComparable, V extends Writable>
					extends CombineHiveInputFormat<WritableComparable, Writable> {

	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		InputSplit[] iss = super.getSplits(job, numSplits);
		InputSplit[] formatedIss = new InputSplit[3]; 
		Map<String, String> sliceid2host = new HashMap<String, String>();
		sliceid2host.put("1", "k1211.mzhen.cn");
		sliceid2host.put("2", "k1216.mzhen.cn");
		sliceid2host.put("3", "k1226.mzhen.cn");
		Map<String, List<SplitInfo>> splitGroups = new HashMap<String, List<SplitInfo>>();
		
		for(int i = 0; i < iss.length; ++i)	{
			InputSplit is = iss[i];
			if(is instanceof CombineHiveInputSplit)	{
				CombineHiveInputSplit hsplit = (CombineHiveInputSplit) is;
				Path[] files = hsplit.getPaths();
				long[] starts = hsplit.getStartOffsets();
				long[] lengths = hsplit.getLengths();
				for(int j = 0; j < files.length; ++j)	{
					SplitInfo splitInfo = new SplitInfo(files[j], starts[j], lengths[j]);
					List<SplitInfo> splitInfos = splitGroups.get(splitInfo.getSliceid());
					if(splitInfos == null)
						splitInfos = new ArrayList<SplitInfo>();
					splitInfos.add(splitInfo);
					splitGroups.put(splitInfo.getSliceid(), splitInfos);
				}
			}
			else	{
				System.out.println("#JUDKING: InputSplit is not CombineHiveInputSplit. InputSplit=["+is+"]");
			}
		}
		
		try	{
			System.out.println("#JUDKING: splitGroups=["+splitGroups+"]");
		}catch(Exception e)	{
			e.printStackTrace();
		}
		List<InputSplit> rtn = new ArrayList<InputSplit>();
		for(Entry<String, List<SplitInfo>> entry : splitGroups.entrySet())	{
			String sliceid = entry.getKey();
			List<SplitInfo> splitInfos = entry.getValue();
			System.out.println("#JUDKING: sliceid=["+sliceid+"], List<SplitInfo>=["+splitInfos+"]");
			Path[] files = new Path[splitInfos.size()];
			long[] starts = new long[splitInfos.size()];
			long[] lengths = new long[splitInfos.size()];
			for(int i = 0; i < splitInfos.size(); ++i)	{
				SplitInfo si = splitInfos.get(i);
				files[i] = si.getFile();
				starts[i] = si.getStart();
				lengths[i] = si.getLength();
			}
			String[] locations = new String[1];
			locations[0] = sliceid2host.get(sliceid);
			org.apache.hadoop.mapred.lib.CombineFileSplit cfs = 
										new org.apache.hadoop.mapred.lib.CombineFileSplit(
															job, 
															files, 
															starts, 
															lengths, 
															locations);
			org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim iqo = 
										new org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim(cfs);
			CombineHiveInputSplit chis = new CombineHiveInputSplit(job, iqo);
			rtn.add(chis);
		}
		
		return rtn.toArray(new InputSplit[rtn.size()]);
	}
	
//	@Override
//	public InputSplit[] getSplits(JobConf job, int numSplits)
//			throws IOException {
//		InputSplit[] iss = super.getSplits(job, numSplits);
//		
//		//---TEST---
//		Path file = null;
//		long start = 0;
//		long length = 0;
//		
//		for(int i = 0; i < iss.length; ++i)	{
//			InputSplit is = iss[i];
//			if(is instanceof CombineHiveInputSplit)	{
////				System.out.println("#JUDKING: Is CombineHiveInputSplit");
////				CombineHiveInputSplit hsplit = (CombineHiveInputSplit) is;
////				InputSplitShim isshim = hsplit.getInputSplitShim();
//				
////				org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim iqo = new org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim(old);
////				org.apache.hadoop.mapred.lib.CombineFileSplit cfs = 
//				
////				long len = hsplit.getLength();
////				List<String> locs = Arrays.asList(hsplit.getLocations());
////				System.out.println("#JUDKING: InputSplit=["+i+"], len=["+len+"], locations=["+locs+"], InputSplitShim=["+isshim.getClass().getCanonicalName()+"]");
////				Path[] paths = hsplit.getPaths();
////				
////				List<String> pathUrls = new ArrayList<String>();
////				for(int j = 0; j < paths.length; ++j)	{
////					pathUrls.add(paths[j].toUri().toString());
////				}
////				System.out.println("#JUDKINg: InputSplit=["+i+"], paths=["+pathUrls+"], getLength=["+hsplit.getLength()+"], getStartOffsets=["+Arrays.asList(ArrayUtils.toObject(hsplit.getStartOffsets()))+"], getLengths=["+Arrays.asList(ArrayUtils.toObject(hsplit.getLengths()))+"]");
////				System.out.println("#JUDKING: toString=["+hsplit.toString()+"]");
//				
//				CombineHiveInputSplit hsplit = (CombineHiveInputSplit) is;
//				
//				//---TEST---
//				if(i == 0)	{
//					Path[] files = hsplit.getPaths();
//					file = files[0];
//					files = Arrays.copyOfRange(files, 1, files.length);
//					long[] starts = hsplit.getStartOffsets();
//					start = starts[0];
//					starts = Arrays.copyOfRange(starts, 1, starts.length);
//					long[] lengths = hsplit.getLengths();
//					length = lengths[0];
//					lengths = Arrays.copyOfRange(lengths, 1, lengths.length);
//					String[] locations = hsplit.getLocations();
//					
//					org.apache.hadoop.mapred.lib.CombineFileSplit cfs = 
//															new org.apache.hadoop.mapred.lib.CombineFileSplit(
//																	job, 
//																	files, 
//																	starts, 
//																	lengths, 
//																	locations);
//					org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim iqo = 
//														new org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim(cfs);
//					CombineHiveInputSplit chis = new CombineHiveInputSplit(job, iqo);
//					iss[i] = chis;
//					System.out.println("#JUDKING: moving things: file=["+file+"], start=["+start+"], length=["+length+"]");
//				}
//				else if(i == 1)	{
//					Path[] files = hsplit.getPaths();
//					files = org.apache.commons.lang3.ArrayUtils.add(files, file);
//					try{
//					}catch(Exception e)	{
//						e.printStackTrace();
//					}
//					long[] starts = hsplit.getStartOffsets();
//					starts = org.apache.commons.lang3.ArrayUtils.add(starts, start);
//					long[] lengths = hsplit.getLengths();
//					lengths = org.apache.commons.lang3.ArrayUtils.add(lengths, length);
//					String[] locations = hsplit.getLocations();
//					
//					org.apache.hadoop.mapred.lib.CombineFileSplit cfs = 
//															new org.apache.hadoop.mapred.lib.CombineFileSplit(
//																	job, 
//																	files, 
//																	starts, 
//																	lengths, 
//																	locations);
//					org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim iqo = 
//							new org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim(cfs);
//					CombineHiveInputSplit chis = new CombineHiveInputSplit(job, iqo);
//					iss[i] = chis;
//				}
//				sysout((CombineHiveInputSplit)iss[i], i);
//			}
//
//		}
//		return iss;
//	}
	

	private void sysout(CombineHiveInputSplit hsplit, int i) throws IOException	{
		long len = hsplit.getLength();
		List<String> locs = Arrays.asList(hsplit.getLocations());
		System.out.println("#JUDKING: InputSplit=["+i+"], len=["+len+"], locations=["+locs+"], InputSplitShim=["+hsplit.getInputSplitShim().getClass().getCanonicalName()+"]");
		Path[] paths = hsplit.getPaths();
		
		System.out.println("#JUDKING: toString=["+hsplit.toString()+"]");
		List<String> pathUrls = new ArrayList<String>();
		for(int j = 0; j < paths.length; ++j)	{
			pathUrls.add(paths[j].toUri().toString());
		}
		System.out.println("#JUDKINg: InputSplit=["+i+"], paths=["+pathUrls+"], getLength=["+hsplit.getLength()+"], getStartOffsets=["+Arrays.asList(ArrayUtils.toObject(hsplit.getStartOffsets()))+"], getLengths=["+Arrays.asList(ArrayUtils.toObject(hsplit.getLengths()))+"]");

	}

}
