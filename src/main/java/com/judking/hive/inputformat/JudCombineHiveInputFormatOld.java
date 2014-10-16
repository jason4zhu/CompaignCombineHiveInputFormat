package com.judking.hive.inputformat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class JudCombineHiveInputFormatOld<K extends WritableComparable, V extends Writable>
					extends CombineHiveInputFormat<WritableComparable, Writable> {
	
	private final String META_FILE = "admonitor.meta";
    private final String META_FILE_ADDRESS = "/admonitor/metafile/meta_file.txt";
    private final String FILE_PART = "part-r";
    private Map<String,String> slice2host = null;
    public static final Log LOG = LogFactory.getLog(JudCombineHiveInputFormatOld.class.getName());
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		InputSplit[] iss = super.getSplits(job, numSplits);
		
		//获得sliceid对应host的Map
		FileSystem fs = FileSystem.get(job);
		if(slice2host==null) {
			String  metapath= job.get(META_FILE,META_FILE_ADDRESS);
			FSDataInputStream fsi = fs.open(new Path(metapath));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsi));
			String line=null;
			slice2host = new HashMap<String,String>();
			while((line=br.readLine())!=null){
				String[] kv = line.split(",");
				if(kv.length==2)
					slice2host.put(kv[0],kv[1]);
			}
		}
		
		//将返回的InputSplit按照sliceid进行重组
		Map<String, List<SplitInfo>> splitGroups = new HashMap<String, List<SplitInfo>>();
		for(int i = 0; i < iss.length; ++i)	{
			InputSplit is = iss[i];
			if(is instanceof CombineHiveInputSplit)	{
				CombineHiveInputSplit hsplit = (CombineHiveInputSplit) is;
				Path[] files = hsplit.getPaths();
				long[] starts = hsplit.getStartOffsets();
				long[] lengths = hsplit.getLengths();
				String inputFormatClassName = hsplit.inputFormatClassName();
				for(int j = 0; j < files.length; ++j)	{
					SplitInfo splitInfo = null;
					try	{
						splitInfo = new SplitInfo(inputFormatClassName, files[j], starts[j], lengths[j], slice2host.size());
					} catch(Exception e)	{
						//System.out.println("#JUDKING_ERROR: Generating SplitInfo fails. path=["+files[j].toUri()+"], errMsg=["+e.getMessage()+"]");
						LOG.error("#JUDKING_FLAG_1: inputSplitSize=["+iss.length+"], errMsg=["+e.getMessage()+"]");
						for(InputSplit iis : Arrays.asList(iss))	{
							LOG.error("InputSplit=["+iis.toString()+"]");
						}
						return iss;
					}
					if(slice2host.containsKey(splitInfo.getSliceid()) == false)	{
						System.out.println("#JUDKING_ERROR: sliceid is not in splitInfo. path=["+files[j].toUri()+"], sliceid=["+splitInfo.getSliceid()+"]");
						continue;
					}
					List<SplitInfo> splitInfos = splitGroups.get(splitInfo.getSliceid());
					if(splitInfos == null)
						splitInfos = new ArrayList<SplitInfo>();
					splitInfos.add(splitInfo);
					splitGroups.put(splitInfo.getSliceid(), splitInfos);
				}
			}
			else	{
				System.out.println("#JUDKING_ERROR: InputSplit is not CombineHiveInputSplit. InputSplit=["+is+"]");
			}
		}
		
		try	{
			//System.out.println("#JUDKING: splitGroups=["+splitGroups+"]");
		}catch(Exception e)	{
			e.printStackTrace();
		}

		//根据重组后的SplitInfo，构造新的InputSplit数组
		List<InputSplit> rtn = new ArrayList<InputSplit>();
		for(Entry<String, List<SplitInfo>> entry : splitGroups.entrySet())	{
			String sliceid = entry.getKey();
			List<SplitInfo> splitInfos = entry.getValue();
			splitInfos = SplitInfo.mergeSplitFiles(splitInfos, slice2host.size());
			//System.out.println("#JUDKING: sliceid=["+sliceid+"], List<SplitInfo>=["+splitInfos+"]");
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
			locations[0] = slice2host.get(sliceid);
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



	/**
	 * 测试用例，将hive_combine_test表中的数据按照part-i分给指定的mapper。
	 * @param hsplit
	 * @param i
	 * @throws IOException
	 */
//	@Override
//	public InputSplit[] getSplits(JobConf job, int numSplits)
//			throws IOException {
//		InputSplit[] iss = super.getSplits(job, numSplits);
//		InputSplit[] formatedIss = new InputSplit[3]; 
//		Map<String, String> sliceid2host = new HashMap<String, String>();
//		sliceid2host.put("1", "k1211.mzhen.cn");
//		sliceid2host.put("2", "k1216.mzhen.cn");
//		sliceid2host.put("3", "k1226.mzhen.cn");
//		Map<String, List<SplitInfo>> splitGroups = new HashMap<String, List<SplitInfo>>();
//		
//		for(int i = 0; i < iss.length; ++i)	{
//			InputSplit is = iss[i];
//			if(is instanceof CombineHiveInputSplit)	{
//				CombineHiveInputSplit hsplit = (CombineHiveInputSplit) is;
//				Path[] files = hsplit.getPaths();
//				long[] starts = hsplit.getStartOffsets();
//				long[] lengths = hsplit.getLengths();
//				for(int j = 0; j < files.length; ++j)	{
//					SplitInfo splitInfo = new SplitInfo(files[j], starts[j], lengths[j]);
//					List<SplitInfo> splitInfos = splitGroups.get(splitInfo.getSliceid());
//					if(splitInfos == null)
//						splitInfos = new ArrayList<SplitInfo>();
//					splitInfos.add(splitInfo);
//					splitGroups.put(splitInfo.getSliceid(), splitInfos);
//				}
//			}
//			else	{
//				System.out.println("#JUDKING: InputSplit is not CombineHiveInputSplit. InputSplit=["+is+"]");
//			}
//		}
//		
//		try	{
//			System.out.println("#JUDKING: splitGroups=["+splitGroups+"]");
//		}catch(Exception e)	{
//			e.printStackTrace();
//		}
//		List<InputSplit> rtn = new ArrayList<InputSplit>();
//		for(Entry<String, List<SplitInfo>> entry : splitGroups.entrySet())	{
//			String sliceid = entry.getKey();
//			List<SplitInfo> splitInfos = entry.getValue();
//			System.out.println("#JUDKING: sliceid=["+sliceid+"], List<SplitInfo>=["+splitInfos+"]");
//			Path[] files = new Path[splitInfos.size()];
//			long[] starts = new long[splitInfos.size()];
//			long[] lengths = new long[splitInfos.size()];
//			for(int i = 0; i < splitInfos.size(); ++i)	{
//				SplitInfo si = splitInfos.get(i);
//				files[i] = si.getFile();
//				starts[i] = si.getStart();
//				lengths[i] = si.getLength();
//			}
//			String[] locations = new String[1];
//			locations[0] = sliceid2host.get(sliceid);
//			org.apache.hadoop.mapred.lib.CombineFileSplit cfs = 
//										new org.apache.hadoop.mapred.lib.CombineFileSplit(
//															job, 
//															files, 
//															starts, 
//															lengths, 
//															locations);
//			org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim iqo = 
//										new org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim(cfs);
//			CombineHiveInputSplit chis = new CombineHiveInputSplit(job, iqo);
//			rtn.add(chis);
//		}
//		
//		return rtn.toArray(new InputSplit[rtn.size()]);
//	}
	
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
