package Search;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/*
 * class:MP4SearchEngine
 * description: the class for search the titles from the output of Hadoop Job  
 * Author: Weijie Liu, Hongwei Wang
 * 
 */

public class MP4SearchEngine {
	
	//static String Inverted_Index_File = "/home/hadoop2/Downloads/starter/hadoop-1.0.4/"+ "output/part-r-00000";
	//static String Parse_Folder = "/home/hadoop2/Downloads/starter/hadoop-1.0.4/input_sample_wiki";
	static String Inverted_Index_File = "/Users/WeijieLiu/Downloads/MP4data/big_inverted_index/part-r-00000";
	static String Parse_Folder = "/Users/WeijieLiu/Downloads/MP4data/big_title_text";
	
	static String Inverted_Index_File_20 = "/Users/WeijieLiu/Downloads/MP4data/big_inverted_index/part-r-00000";
	static String Parse_Folder_20 = "/Users/WeijieLiu/Downloads/MP4data/big_title_text";
	
	static String Inverted_Index_File_1 = "/Users/WeijieLiu/Downloads/MP4data/sample_inverted_index/part-r-00000";
	static String Parse_Folder_1 = "/Users/WeijieLiu/Downloads/MP4data/sample_title_text";
	
	static final String Test_File = "test.txt";
	static final int CHAR_LEN = 1;
	static final String file_offset_sep = "<>";
	static final String offset_freq_sep = ":";
	
	//Part I: binary search to get the index list of a certain input
	/*
	 * input: a word and the inverted index file; 
	 * output: the title and frequency list of that word 
	 */
	
	static class entry
	{
		Long left;
		Long right;
		entry(){}
		entry(Long l, Long r)
		{
			left = l;
			right = r;
		}
	}
	
	
	
	static char bytesstr(byte[] bytes)
	{
		//char tmp = (char)((bytes[0]&0x00FF)<<8 + (bytes[1])&0x00FF);
		char tmp = (char)(bytes[0]&0x00FF);
		return tmp;
	}
	
	static entry get_index_of_record(long mid, RandomAccessFile ran)
	{
		byte[] bytes = new byte[CHAR_LEN];
		int bytesread = 0;
		long origin_mid = mid;
		long left=0;
		long right=0;
		
		try {
			while(mid<ran.length())
			{
				ran.seek(mid);// the right \n
				
				if((bytesread=ran.read(bytes))==CHAR_LEN)
				{
					char tmp = bytesstr(bytes);
					if(tmp=='\n')
					{
						right=mid-CHAR_LEN; // no \n is needed
						break;
					}else
					{
						mid+=CHAR_LEN;
					}
					
				}else{
					
					break;
				}
			}
			
			if(mid>=ran.length()-1) right = ran.length()-1;
			
			mid = origin_mid-CHAR_LEN;
			
			while(mid>0)
			{
				ran.seek(mid);// the left \n 
				
				if((bytesread=ran.read(bytes))==CHAR_LEN)
				{
					char tmp = bytesstr(bytes);
					if(tmp=='\n')
					{
						left=mid+CHAR_LEN; // no \n is needed
						break;
					}else
					{
						mid-=CHAR_LEN;
					}
					
				}else{
					break;
				}
			}
			
			if(mid<=0) left=(long)0; 
			
			entry res = new entry(left,right);
			return res;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	static String get_line(entry index, RandomAccessFile ran)
	{
		String res = null;
		
		long len = index.right-index.left+CHAR_LEN;
		
		byte[] buf = new byte[(int) len];
		char[] chararray = new char[(int)(len/CHAR_LEN)];
		
		try {
			ran.seek(index.left);
			int bytesread = ran.read(buf);
			
			for(int i=0;i<buf.length;i=i+CHAR_LEN)
			{
				//chararray[i/CHAR_LEN] = (char)((buf[i]&0x00FF)<<8 + (buf[i+1])&0x00FF);
				chararray[i/CHAR_LEN] = (char)(buf[i]);
			}
			
			return new String(chararray);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res;
	}
	
	static String binary_search(String word, RandomAccessFile ran)
	{
		long left = 0;
		long mid;
		
		try {
			long right = ran.length()-1;
			
			ran.seek(right);
			byte eof = ran.readByte();
			if(eof==10) --right;
			
			if(right<2)
			{
				System.out.println("binary_search: file too short");
			}
			
			while(left<=right)
			{
				mid = (left + right)/2;
				//System.out.println("left:"+left+" ,right"+right);
				
				
				//if(mid%2 == 1) --mid;
				
				entry index_line = get_index_of_record(mid,ran); // get the index of that record
				//System.out.println("left:"+index_line.left+" right:"+index_line.right);
				
				String line = get_line(index_line,ran); // one line of record
				
				String[] split = line.split("\t");
				//System.out.println(line);
				
				if(split.length<2)
				{
					System.out.println("binary_search: too short record in Inverted_Index_file");
					
				}
				
				if(split[0].equals(word))
				{
					return line;
				}else if(split[0].compareTo(word)<0)
				{
					left = mid+1;
					//System.out.println("");
				}else
				{
					right = mid -1;
				}
				
				//break; //***
			}
							
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // in bytes
		
		return null; // no word found return null
	}
	
	//Part II: combine the results if multiple inputs
	/*
	 * input: the lists of file_offset and freq of one or multiple words
	 * output: top K results (e.g. sorted by min freq) in this lists
	 */
	
	static ArrayList<page> binary_search_multiple(ArrayList<String> words, RandomAccessFile ran)
	{
		ArrayList<page> res = new ArrayList<page>();
		
		HashMap<String, page> freq_map = new HashMap<String, page>();
		
		for(String word : words)
		{
			String record = binary_search(word, ran);
			if(record==null) return null; // if one word can not be found, no results should be displayed
			page p;
			
			String[] split = record.split("\t");
			if(split.length<2)
			{
				System.out.println("binary_search_multiple: too short record");
				continue;
			}
			
			//e.g. asdfasdf.txt<>87897:10000
			
			for(int i=1;i<split.length;i++)
			{
				if(split[i].equals("")||split[i].equals(" ")||split[i].equals("\n")) continue;
				
				String[] offset_freq = split[i].split(offset_freq_sep);
				if(offset_freq.length<2)
				{
					System.out.println("binary_search_multiple: too short offset_freq " );
					continue;
				}
				
				if(freq_map.containsKey(offset_freq[0]))
				{
					p = freq_map.get(offset_freq[0]);
					p.addHits(1);
					p.addFreq(Integer.parseInt(offset_freq[1]));
					
				}else
				{
					p = new page(offset_freq[0]);
					p.addHits(1);
					p.addFreq(Integer.parseInt(offset_freq[1]));
					freq_map.put(offset_freq[0], p);
				}
			}
		}
		
		//ArrayList<page> pages_contain_all = new ArrayList<page>();
		page tmp;
		
		for(String title : freq_map.keySet())
		{
			tmp = freq_map.get(title);
			if(tmp.hits == words.size())
			{
				res.add(tmp);
			}
		}
		
		//sort the result
		Collections.sort(res,new Comparator<page>(){
			public int compare(page p1, page p2){
				if(p1.comb_freq==p2.comb_freq)
				{
					return 0;
				}else if(p1.comb_freq<p2.comb_freq)
				{
					return 1; //from big to small
				}else
				{
					return -1; 
				}
			}
		});
		
		return res;
	}
	
	
	
	
	static ArrayList<page> toPageList(String record)
	{
		//System.out.println(record);
		
		ArrayList<page> res = new ArrayList<page>();
		
		if(record==null) return null;
		
		String[] split = record.split("\t");
		
		if(split.length<2)
		{
			System.out.println("toPageList: too short record");
			return null;
		}
		
		for(int i=1;i<split.length;i++)
		{
			if(split[i].equals("")||split[i].equals("\n")) continue;
			
			String[] offset_freq = split[i].split(offset_freq_sep);
			page tmp = new page(offset_freq[0]);
			tmp.addFreq(Integer.parseInt(offset_freq[1]));
			res.add(tmp); // has sorted in reduce
		}
		
		return res;
	}
	
	//Part III: translate the filename and offset into the title of that page
	/*
	 * input: the parsed file and the filename and offset
	 * output: the titles
	 */
	
	static String display_title(String files_folder, ArrayList<page> page_list)
	{
		StringBuilder res = new StringBuilder();
		int i=1;
		
		for(page p : page_list)
		{
			String[] file_offset = p.title.split(file_offset_sep);
			long offset = Long.parseLong(file_offset[1]);
			res.append("("+(i++)+")"+"\t"+get_title(Parse_Folder+"/"+file_offset[0],offset)+"\n");
			
			if(i>10) break;
			
		}
		
		return res.toString();
	}
	
	static char char_at(RandomAccessFile ran, long p)
	{
		
		byte bt;
		try {
			ran.seek(p);
			bt = ran.readByte();
			char tmp = (char) bt;
			return tmp;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 6;
	}
	
	static String get_title(String path, long offset)
	{
		RandomAccessFile ran;
		long p = offset;
		
		try {
			
			ran = new RandomAccessFile(path,"r");
			
			while(p<ran.length())
			{
						
				if(char_at(ran,p)=='<' && p+3<ran.length() && char_at(ran,p+1)=='_' &&
						char_at(ran,p+2)=='_'&&char_at(ran,p+3)=='>')
				{
					//System.out.println("here is it, p="+p);
					
					entry index =new entry();
					index.left = offset;
					index.right = p - CHAR_LEN;
					
					String title = get_line(index, ran); //get title 
					//System.out.println("title shoule be: "+ title);
					return title;
					
				}
				
				++p;
			}
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	
	// main function
	
	public static void main(String [] args)
	{
		System.out.println("****************************************");
		System.out.println("CS423 G3 MP4: Wikipia Search Page Engine");
		System.out.println("Copyright @ Weijie Liu, Hongwei Wang");
		System.out.println("*************************************");
		
		ArrayList<String> words = new ArrayList<String>();
		
		
		if(args.length == 2)
		{
			MP4SearchEngine.Inverted_Index_File = args[0];
			MP4SearchEngine.Parse_Folder = args[1];
		}else if(args.length == 1)
		{
			MP4SearchEngine.Inverted_Index_File = args[0];
		}
		
		RandomAccessFile ran;
		
		
		while(true)
		{
			System.out.println("To use default 20.xml, please input 20; use default 1.xml, please intput 1; otherwise, "
					+ "otherwise, please input any other string");
			
			BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
			
			String tmp = "y";
			
			try {
				tmp = bf.readLine();
				
				if(tmp.equals("20"))
				{
					System.out.println("Use 20.xml: ");
					MP4SearchEngine.Inverted_Index_File = MP4SearchEngine.Inverted_Index_File_20;
					MP4SearchEngine.Parse_Folder = MP4SearchEngine.Parse_Folder_20;
				}else if (tmp.equals("1"))
				{
					System.out.println("Use 1.xml: ");
					MP4SearchEngine.Inverted_Index_File = MP4SearchEngine.Inverted_Index_File_1;
					MP4SearchEngine.Parse_Folder = MP4SearchEngine.Parse_Folder_1;
				}else
				{
					System.out.println("Use other file: ");
					System.out.println("please input the path and name of Inverted Index: ");
					MP4SearchEngine.Inverted_Index_File = bf.readLine();
					System.out.println("please input the path and name of the folder where Parse Files locate: ");
					MP4SearchEngine.Parse_Folder = bf.readLine();
				}
				
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			
			System.out.println("  Inverted Index source: "+MP4SearchEngine.Inverted_Index_File);
			System.out.println("  Parsed pages in folder: "+MP4SearchEngine.Parse_Folder);
			
			try {
				ran = new RandomAccessFile(Inverted_Index_File,"r");
				//ran = new RandomAccessFile(Test_File,"r");
				
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return;
			}
			
			System.out.println("===============================================================================");
			System.out.println("Please input one or more words (finish input with !, exit the program with *): ");
			
			words.clear();
			
			while(true)
			{
				try {
					String str = bf.readLine();
					if(str.equals("!"))
					{
						break;
					}else if(str.equals("*"))
					{
						System.out.println("Search Engine Exits");
						return;
					}else
					{
						words.add(str.toLowerCase().trim());
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(words.size()==1)
			{
				String record = binary_search(words.get(0).trim(),ran);
				
				if(record==null)
				{
					System.out.println("	Sorry, no page has this word: "+words.get(0));
					continue;
				}
				
				//System.out.println(record);
				System.out.println("The search result should be:\n");
				
				System.out.println(display_title(Parse_Folder,toPageList(record)));
				
			}else
			{
				ArrayList<page> pages = binary_search_multiple(words,ran);
				if(pages==null)
				{
					System.out.println("	Sorry, no page has this words ");
					continue;
				}
				
				/*
				int i=0;
				for(page p: record)
				{
					System.out.println((++i)+":"+p);
				}*/
				
				System.out.println("  The search result should be:");
				
				System.out.println(display_title(Parse_Folder,pages));
			}
			
		}
	}
}


// page 

class page
{
	String title;
	//String text;
	
	int hits;
	int comb_freq;
	
	page(){}
	page(String s){title=s;hits=0;this.comb_freq=0;}
	page(String s, int frq){title=s;comb_freq = frq;this.hits=0;}
	
	public int addHits(int h)
	{
		this.hits+=h;
		return this.hits;
	}
	
	public int addFreq(int f)
	{
		this.comb_freq+=f;
		return this.comb_freq;
	}
	
	/*
	public boolean equals(Object ob)
	{
		if(!(ob instanceof page)) return false;
		
		page p = (page) ob;
		
		if(p.)
	}*/
	
	public String toString()
	{
		return this.title+":"+this.comb_freq;
	}
	
}
