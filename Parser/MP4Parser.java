package Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/*
	MP4Parser: the class to parse the XML file into the format with:
		(1) <__> as the seperator of title and text
		(2)	<eof> as the seperator of the pages
*/

public class MP4Parser {
	String Delimiter_Title_Text="<__>";
	String Delimiter_Page_Page="<eof>";
	
	String input_xml = "/Users/WeijieLiu/Downloads/data/20.xml";
	
	int titleDlm;
	int pageDlm;
	int resNum;
	String XMLPath;
	ArrayList<PageBean> resList;
	
	MP4Parser(String path,int resNum)
	{
		this.titleDlm=6;
		this.pageDlm=0;
		
		//Character ttsep=6;
		//Delimiter_Title_Text=ttsep.toString();
		
		//Character ppsep=5;
		//Delimiter_Page_Page=ppsep.toString();
		
		this.resNum=resNum;
		this.XMLPath=path;
	}
	
	MP4Parser(String path,int resNum,int pDlm,int tDlm)
	{
		this.titleDlm=tDlm;
		this.pageDlm=pDlm;
		
		this.resNum=resNum;
		this.XMLPath=path;
	}
	
	public void ParseXML()
	{
		//MP4Parser.class.getResourceAsStream
	     
		try {
			this.resList=readXmlByStAX(new FileInputStream(this.XMLPath));
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ArrayList<PageBean> readXmlByStAX(InputStream stream)
	 throws XMLStreamException
	{
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader reader = factory.createXMLStreamReader(stream);
		ArrayList<PageBean> list = new ArrayList<PageBean>();
		PageBean bean=null;
		
		try
		  {
		   int event = reader.getEventType();

		   while (true)
		   {
		    switch (event)
		    {
		    case XMLStreamConstants.START_DOCUMENT:
		     // System.out.println("Start Document.");
		     break;
		    case XMLStreamConstants.START_ELEMENT:
		     // System.out.println("Start Element: " + reader.getName());
		     if (reader.getLocalName().equals("page"))
		     {
		      bean = new PageBean();
		     }
		     
		     if (reader.getLocalName().equals("title"))
		     {
		      bean.setTitle(reader.getElementText());
		     } else if (reader.getLocalName().equals("text"))
		     {
		      bean.setText(reader.getElementText());
		     } 
		     // if (reader.getLocalName().equals("title"))
		     // {
		     // for (int i = 0, n = reader.getAttributeCount(); i < n;
		     // ++i)
		     // System.out.println("Attribute: "
		     // + reader.getAttributeName(i) + "="
		     // + reader.getAttributeValue(i));
		     // }

		     break;
		    case XMLStreamConstants.CHARACTERS:
		     if (reader.isWhiteSpace())
		      break;

		     // System.out.println("Text: " + reader.getText());
		     break;
		    case XMLStreamConstants.END_ELEMENT:
		     if(reader.getLocalName().equals("page"))
		     {
		      list.add(bean);
		     }
		     // System.out.println("End Element:" + reader.getName());
		     break;
		    case XMLStreamConstants.END_DOCUMENT:
		     // System.out.println("End Document.");
		     break;
		    }
		    if (!reader.hasNext())
		     break;

		    event = reader.next();
		   }
		  } finally
		  {
		   reader.close();
		  }
		
		
		return list;
	}
	
	public void writeResToFile()
	{
		int pageNumInFile = this.resList.size()/this.resNum;
		
		//write results to the N files
		
		for(int i=0;i<this.resNum;i++)
		{
			String file_name = String.valueOf(i);
			if(file_name.length()<2) file_name = "0"+file_name;
			this.writeResToOneFile(file_name, i*pageNumInFile, (i+1)*pageNumInFile-1);
		}
	}
	
	private void writeResToOneFile(String fileName, int sat, int end)
	{
		try {
			FileOutputStream fos = new FileOutputStream(new File(fileName));
			String title;
			String text;
			
			for(int i=sat;i<=end&&i<this.resList.size();i++)
			{
				title = this.resList.get(i).getTitle();
				text = this.resList.get(i).getText();
				
				fos.write(title.getBytes());
				fos.write(this.Delimiter_Title_Text.getBytes());
				
				fos.write(text.getBytes());
				fos.write(this.Delimiter_Page_Page.getBytes());
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		
		if(args.length<2)
		{
			System.out.println("Error: the length of input is less than 2");
		}
		
		MP4Parser parser=null;
		
		if(args.length==2)
		{
			parser = new MP4Parser(args[0],Integer.parseInt(args[1]));
		}else if(args.length==4)
		{
			parser = new MP4Parser(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2]),
					Integer.parseInt(args[3]));
		}
		
		if(parser!=null)
		{
			System.out.println("Main: Begin to parse the XML: "+ args[0]);
			parser.ParseXML();
			parser.writeResToFile();
			System.out.println("Main: generate "+args[1]+ " files");
		} 
		//testContain(0);
	}
	
	public static void testContain(int num)
	{
		Character sep = (char) num;
		String sepp = sep.toString();
		
		try {
			BufferedReader bf=  new BufferedReader(new InputStreamReader(new FileInputStream(new File("aaa_0.txt"))));
			String line = null;
			int i=0;
			
			while((line=bf.readLine())!=null)
			{
				if(line.contains(sepp)) System.out.println(++i);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

class PageBean{
	private String title;
	private String text;
	
	public String getTitle()
	{
		return this.title;
	}
	
	public String getText()
	{
		return this.text;
	}
	
	public void setTitle(String t)
	{
		this.title=t;
	}
	
	public void setText(String tx)
	{
		this.text=tx;
	}
	
}
