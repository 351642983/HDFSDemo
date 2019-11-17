package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;





/*
 * Hadoop文件和本地文件监听之Hadoop文件操作类 V1.02
 * V1.00:Hadoop文件的增删改
 * V1.01:Hadoop文件的监听
 * V1.02:增加了下载和上传的阻塞,使数据变得同步
 */



public class HDFSDemo {

	public final String hd_uri = "hdfs://192.168.57.128:9000";
	public final String hd_owner = "hadoop";
	public final int g_checktime=500;



	private Timer timer;
	private boolean g_first;

	private String strChangeTime;
	//启动检测任务，对文件进行监听同时绑定到hadoop对应的任务上
	public void startTimer(String path,String webpath)
	{
		g_first=true;
		timer = new Timer(true); 
		timer.schedule(new java.util.TimerTask() 
		{ 
			public void run()
			{ 
				checkFileToUpdate(path,webpath); 
			} 
		}, 0, g_checktime);

	}




	//检测文件是否符合上传标准，是的话对文件进行上传操作
	public void checkFileToUpdate(String path,String webpath)
	{
		//System.out.println("触发检查");
		if(!g_first)
		{
			if(judgeFileExists(path))
			{
				String date=getModifiedTime(path);
				if(!strChangeTime.equals(date))
				{
					try {
						if(judgeFileExists(path))
						{
							System.out.println("上传1:"+path);
							strChangeTime=date;
							updateFile(path,webpath);

							System.out.println("上传等待结束");

						}
						else
						{

							if(isFileSystemExist(webpath))
							{
								downloadFile(webpath,path);
								System.out.println("下载2:"+webpath);

								System.out.println("下载等待结束");
							}
							else
							{
								outFileToLocal("",path,false);
							}
							strChangeTime=getModifiedTime(path);
						}
					} catch (IOException  e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
				}
			}
			else
			{
				if(isFileSystemExist(webpath))
				{
					downloadFile(webpath,path);
					System.out.println("下载3:"+webpath);

					System.out.println("下载等待结束");
				}
				else
				{
					try {
						outFileToLocal("",path,false);
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
				}
				strChangeTime=getModifiedTime(path);
			}
		}
		else
		{
			g_first=false;
			try {
				if(isFileSystemExist(webpath))
				{
					downloadFile(webpath,path);
					System.out.println("下载1:"+webpath);
					strChangeTime=getModifiedTime(path);

					System.out.println("下载等待结束");
				}
				else
				{
					if(!judgeFileExists(path))
					{
						outFileToLocal("",path,false);
					}
					else
					{
						System.out.println("上传2:"+path);
						updateFile(path,webpath);
						strChangeTime=getModifiedTime(path);

						System.out.println("上传等待结束");
					}

				}
			} catch (IOException  e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
			strChangeTime=getModifiedTime(path);
		}
	}
	
	

	public void downloadFile(String webpath,String path)
	{
		// 下载文件
		FileSystem fs;
		try {
			FileStatus f=null;
			fs = getFileSystem();
			Path dir = new Path(webpath);
			f= fs.getFileStatus(dir);
			long len = f.getLen();
			File fi=new File(path);

			// 实现HDFS内的文件下载至本地
			InputStream in=fs.open(new Path(webpath));
			// 下载到本地,保存后的名称是hadoop.txt
			OutputStream out=new FileOutputStream(path);
			// 按照字节的方式复制.buffersize是4K,写完后返回true.
			IOUtils.copyBytes(in, out, 4096, true);
			//进程阻塞
			while(len!=fi.length());


			//关闭流
			fs.close();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}

	}
	public void updateFile(String path,String webpath)
	{
		// 上传文件
		FileSystem fs;
		if(!isFileSystemExist(webpath))
		{
			createFile(webpath,new byte[]{});
		}
		try {
			FileStatus f=null;
			fs = getFileSystem();
			File fi=new File(path);
			long len = fi.length();
			f= fs.getFileStatus(new Path(webpath));
			// 实现上传文件,首先是读取本地的文件
			InputStream in=new FileInputStream(path);
			// 上传文件到HDFS的指定目录下.
			OutputStream out=fs.create(new Path(webpath),true);
			// 按照字节的方式复制.buffersize是4K,写完后返回true
			IOUtils.copyBytes(in, out, 4096, true);

			//进程阻塞直到上传完毕

			while(len!=f.getLen())
			{
				f= fs.getFileStatus(new Path(webpath));
			}

			// 关闭流
			fs.close();
		} catch (IOException  e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}


	}

	public boolean deleteFile(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//删除Hdfs上的文件
		FileSystem fs=getFileSystem();
		Path dePath=new Path(webpath);
		boolean isOk=fs.deleteOnExit(dePath);
		return isOk;
	}


	public boolean deleteFileSystem(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//删除目录或文件
		FileSystem fs=getFileSystem();
		// 删除一个文件夹,这里从HDFS的根目录写起的,如果是单个文件就是false,目录下面有目录就true.
		@SuppressWarnings("deprecation")
		Boolean flag=fs.delete(new Path(webpath));
		// 关闭 
		fs.close();
		return flag;
	}

	public boolean createFileSystem(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//创建目录
		FileSystem fs=getFileSystem();
		// 创建一个文件夹,这里从HDFS的根目录写起的.
		Boolean flag=fs.mkdirs(new Path(webpath));
		//关闭流
		fs.close();
		return flag;
	}
	//判断文件是否存在
	public boolean isFileSystemExist(String webpath)
	{
		try {

			FileSystem fs = getFileSystem();
			if (fs.exists(new Path(webpath))){
				fs.close();
				return true;
			}else{
				fs.close();
				return false;
			}
		} catch (IllegalArgumentException | IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		return false;

	}

	public FileSystem getFileSystem() 
	{
		FileSystem fs=null;
		try {
			Configuration conf = new Configuration();
			//conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			fs = FileSystem.get(new URI(hd_uri),conf ,hd_owner);
		} catch (IOException | InterruptedException | URISyntaxException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		return fs;
	}

	/**
	 * 在HDFS中创建一个空文件，并写东西
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 */
	public void createFile(String fileName,byte[] contents) {
		FileSystem fs;
		try {
			fs = getFileSystem();
			Path dst=new Path(fileName);
			FSDataOutputStream fsos=fs.create(dst);
			fsos.write(contents);
			fsos.close();
			fs.close();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}

	}

	/**
	 * 重命名
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 */
	public void rename(String srcName,String dstName) {
		FileSystem fs;
		try {
			fs = getFileSystem();
			Path src=new Path(srcName);
			Path dst=new Path(dstName);
			fs.rename(src, dst);
			fs.close();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}

	}

	/**
	 * 将HDFS上文件复制到HDFS上，可以换名字
	 * @param srcFile
	 * @param dstFile
	 * @throws IOException
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 */
	public void copyFile(String srcFile,String dstFile) {
		FileSystem fs;
		try {
			fs = getFileSystem();
			FSDataInputStream fsis=fs.open(new Path(srcFile));;
			FSDataOutputStream fsos=fs.create(new Path(dstFile));
			byte[] byt=new byte[1024];
			int a=0;
			while((a=fsis.read(byt))!=-1){
				fsos.write(byt,0,a);
			}
			fsis.close();
			fsos.close();
			fs.close();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}

	}

	//获得文件修改的时间
	public String getFileLastTime(String webpath)
	{
		FileSystem fs;
		FileStatus f=null;
		try {
			fs = getFileSystem();
			Path dir = new Path(webpath);
			f= fs.getFileStatus(dir);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}      
		Calendar cal = Calendar.getInstance();  
		String timechange="";
		//获取文件时间
		long time = f.getModificationTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		//转换文件最后修改时间的格式
		cal.setTimeInMillis(time);    

		timechange = formatter.format(cal.getTime());
		return timechange;
	}
	//是否以追加的形式导出文件
	public void outFileToLocal(String txt,String outfile,boolean isappend) throws IOException
	{
		File fi=new File(outfile);
		FileOutputStream fop=new FileOutputStream(fi,isappend);
		OutputStreamWriter ops=new OutputStreamWriter(fop,"UTF-8");
		ops.append(txt);
		ops.close();
		fop.close();
	}
	//判断文件是否存在
	public boolean judgeFileExists(String path) {

		File file=new File(path);
		if (file.exists()) {
			return true;
		} else {
			return false;
		}

	}

	//导入文件时判断文件存在
	public boolean judeFileExistsNoDepend(File file) {

		if (file.exists()) {
			return true;
		} else {
			return false;
		}

	}

	//读取文件每一行的数据并且放在字符串容器中
	public List<String> inputFile(String webpath) throws IOException
	{

		List<String> strlist=new ArrayList<String>();
		InputStream b =getFileSystem().open(new Path(webpath));
		InputStreamReader c=new InputStreamReader(b,"UTF-8");


		{
			BufferedReader bufr =new BufferedReader(c);
			String line = null;
			while((line = bufr.readLine())!=null){
				//line是每一行的数据
				strlist.add(line);
			}
			bufr.close();
		}
		c.close();
		b.close();
		return strlist;
	}

	//是否以追加的形式导出文件
	public void outFile(String txt,String webpath,boolean isappend) throws IOException
	{
		
		if(!isappend)
		{
			OutputStream fop=getFileSystem().create(new Path(webpath),true);
			OutputStreamWriter ops=new OutputStreamWriter(fop,"UTF-8");
			ops.append(txt);
			ops.close();
			fop.close();
		}
		else
		{
			List<String> info=inputFile(webpath);
			OutputStream fop=getFileSystem().create(new Path(webpath),true);
			OutputStreamWriter ops=new OutputStreamWriter(fop,"UTF-8");
			StringHandle sh=new StringHandle();
			ops.append((info.size()==0)?txt:(sh.StringListIntoString(info, "\r\n")+"\r\n"+txt));
			ops.close();
			fop.close();
		}
		
	}

	//获得文件的内容行数
	public long getLineNumber(String webpath) {
		if (isFileSystemExist(webpath)) {
			try {
				long lines = inputFile(webpath).size();
				return lines;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}


	//删除文件中的第n行数据
	public String deleteLine(String filePath,int indexLine){             
		try {        
			List<String> ifList=inputFile(filePath);
			ifList.remove(indexLine);
			outFile(new StringHandle().StringListIntoString(ifList, "\r\n"),filePath,false);
		} catch (Exception e) {  
			return "fail :"+ e.getCause();     
		}      
		return "success!";   
	}


	//将字符串容器以行间修饰串存入文件中
	public void outFileByStringList(List<String> strlist,String outfile,String lineDecorate)
	{
		try {
			outFile(new StringHandle().StringListIntoString(strlist, lineDecorate),outfile,false);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	//将字符串容器以空格和行间修饰串存入文件中
	public void outFileByStringListList(List<List<String>> strlist,String outfile,String lineDecorate)
	{
		try {
			StringHandle sh=new StringHandle();
			outFile(sh.StringListIntoString(sh.StringListListIntoStringList(strlist," "),lineDecorate ),outfile,false);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	//获得文件的修改时间
	public String getModifiedTime(String path){  
		File f = new File(path);              
		Calendar cal = Calendar.getInstance();  
		String timechange="";
		//获取文件时间
		long time = f.lastModified();  
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		//转换文件最后修改时间的格式
		cal.setTimeInMillis(time);    

		timechange = formatter.format(cal.getTime());
		return timechange;
		//输出：修改时间[2]    2009-08-17 10:32:38  
	}  

	//判断文件是否为空
	public boolean fileIsEmpty(String path)
	{
		File fi=new File(path);

		if(fi.length()==0||!fi.exists())
			return true;
		else return false;
	}

	//获得文件目录里面的信息转为字符串的容器的容器
	public List<List<String>> getInfosList(String path)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		List<List<String>> objinfo=sh.StringSplitByExpToStringList(objline, " ");;
		return objinfo;
	}

	//获得文件目录里面的信息转为对应类的容器
	public <T> List<T> getInfosToTlist(String path,String []nameNlist,Class<T> clazz)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		List<T> objinfo=sh.StringSplitByExpToTList(objline, " ",nameNlist,clazz);;
		return objinfo;
	}

	//获得文件目录的里面的信息转为对应类的容器(自动型)
	public <T> List<T> getInfosToTlist(String path,Class<T> clazz)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		List<T> objinfo=sh.StringSplitByExpToTList(objline, " ",clazz);;
		return objinfo;
	}

	//将对应类容器里面的所有数据存入对应文件中
	public <T> void outputFileByTlist(List<T> obj,Class<?> clazz,String path,boolean isAppend)
	{
		EntityToString ets=new EntityToString();
		StringHandle sh=new StringHandle();
		try {
			if(fileIsEmpty(path))
				outFile(sh.StringListIntoString(ets.getStringList(obj, clazz),"\r\n"),path,isAppend);
			else outFile("\r\n"+sh.StringListIntoString(ets.getStringList(obj, clazz),"\r\n"),path,isAppend);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}
	public <T> void outputFileByTlist(List<T> obj,String path,boolean isAppend)
	{
		outputFileByTlist(obj,obj.get(0).getClass(),path,isAppend);
	}


	//将对应的类数据添加到对应文件中
	public <T> void outputFileByT(T obj,Class<?> clazz,String path,boolean isAppend)
	{
		EntityToString ets=new EntityToString();
		try {
			if(fileIsEmpty(path))
				outFile(ets.getString(obj, clazz),path,isAppend);
			else outFile("\r\n"+ets.getString(obj, clazz),path,isAppend);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	public <T> void outputFileByT(T obj,String path,boolean isAppend)
	{
		outputFileByT(obj,obj.getClass(),path,isAppend);
	}

}