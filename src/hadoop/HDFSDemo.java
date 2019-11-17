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
 * Hadoop�ļ��ͱ����ļ�����֮Hadoop�ļ������� V1.02
 * V1.00:Hadoop�ļ�����ɾ��
 * V1.01:Hadoop�ļ��ļ���
 * V1.02:���������غ��ϴ�������,ʹ���ݱ��ͬ��
 */



public class HDFSDemo {

	public final String hd_uri = "hdfs://192.168.57.128:9000";
	public final String hd_owner = "hadoop";
	public final int g_checktime=500;



	private Timer timer;
	private boolean g_first;

	private String strChangeTime;
	//����������񣬶��ļ����м���ͬʱ�󶨵�hadoop��Ӧ��������
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




	//����ļ��Ƿ�����ϴ���׼���ǵĻ����ļ������ϴ�����
	public void checkFileToUpdate(String path,String webpath)
	{
		//System.out.println("�������");
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
							System.out.println("�ϴ�1:"+path);
							strChangeTime=date;
							updateFile(path,webpath);

							System.out.println("�ϴ��ȴ�����");

						}
						else
						{

							if(isFileSystemExist(webpath))
							{
								downloadFile(webpath,path);
								System.out.println("����2:"+webpath);

								System.out.println("���صȴ�����");
							}
							else
							{
								outFileToLocal("",path,false);
							}
							strChangeTime=getModifiedTime(path);
						}
					} catch (IOException  e) {
						// TODO �Զ����ɵ� catch ��
						e.printStackTrace();
					}
				}
			}
			else
			{
				if(isFileSystemExist(webpath))
				{
					downloadFile(webpath,path);
					System.out.println("����3:"+webpath);

					System.out.println("���صȴ�����");
				}
				else
				{
					try {
						outFileToLocal("",path,false);
					} catch (IOException e) {
						// TODO �Զ����ɵ� catch ��
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
					System.out.println("����1:"+webpath);
					strChangeTime=getModifiedTime(path);

					System.out.println("���صȴ�����");
				}
				else
				{
					if(!judgeFileExists(path))
					{
						outFileToLocal("",path,false);
					}
					else
					{
						System.out.println("�ϴ�2:"+path);
						updateFile(path,webpath);
						strChangeTime=getModifiedTime(path);

						System.out.println("�ϴ��ȴ�����");
					}

				}
			} catch (IOException  e) {
				// TODO �Զ����ɵ� catch ��
				e.printStackTrace();
			}
			strChangeTime=getModifiedTime(path);
		}
	}
	
	

	public void downloadFile(String webpath,String path)
	{
		// �����ļ�
		FileSystem fs;
		try {
			FileStatus f=null;
			fs = getFileSystem();
			Path dir = new Path(webpath);
			f= fs.getFileStatus(dir);
			long len = f.getLen();
			File fi=new File(path);

			// ʵ��HDFS�ڵ��ļ�����������
			InputStream in=fs.open(new Path(webpath));
			// ���ص�����,������������hadoop.txt
			OutputStream out=new FileOutputStream(path);
			// �����ֽڵķ�ʽ����.buffersize��4K,д��󷵻�true.
			IOUtils.copyBytes(in, out, 4096, true);
			//��������
			while(len!=fi.length());


			//�ر���
			fs.close();
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}

	}
	public void updateFile(String path,String webpath)
	{
		// �ϴ��ļ�
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
			// ʵ���ϴ��ļ�,�����Ƕ�ȡ���ص��ļ�
			InputStream in=new FileInputStream(path);
			// �ϴ��ļ���HDFS��ָ��Ŀ¼��.
			OutputStream out=fs.create(new Path(webpath),true);
			// �����ֽڵķ�ʽ����.buffersize��4K,д��󷵻�true
			IOUtils.copyBytes(in, out, 4096, true);

			//��������ֱ���ϴ����

			while(len!=f.getLen())
			{
				f= fs.getFileStatus(new Path(webpath));
			}

			// �ر���
			fs.close();
		} catch (IOException  e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}


	}

	public boolean deleteFile(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//ɾ��Hdfs�ϵ��ļ�
		FileSystem fs=getFileSystem();
		Path dePath=new Path(webpath);
		boolean isOk=fs.deleteOnExit(dePath);
		return isOk;
	}


	public boolean deleteFileSystem(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//ɾ��Ŀ¼���ļ�
		FileSystem fs=getFileSystem();
		// ɾ��һ���ļ���,�����HDFS�ĸ�Ŀ¼д���,����ǵ����ļ�����false,Ŀ¼������Ŀ¼��true.
		@SuppressWarnings("deprecation")
		Boolean flag=fs.delete(new Path(webpath));
		// �ر� 
		fs.close();
		return flag;
	}

	public boolean createFileSystem(String webpath) throws IOException, InterruptedException, URISyntaxException
	{
		//����Ŀ¼
		FileSystem fs=getFileSystem();
		// ����һ���ļ���,�����HDFS�ĸ�Ŀ¼д���.
		Boolean flag=fs.mkdirs(new Path(webpath));
		//�ر���
		fs.close();
		return flag;
	}
	//�ж��ļ��Ƿ����
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
			// TODO �Զ����ɵ� catch ��
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
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
		return fs;
	}

	/**
	 * ��HDFS�д���һ�����ļ�����д����
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
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}

	}

	/**
	 * ������
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
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}

	}

	/**
	 * ��HDFS���ļ����Ƶ�HDFS�ϣ����Ի�����
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
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}

	}

	//����ļ��޸ĵ�ʱ��
	public String getFileLastTime(String webpath)
	{
		FileSystem fs;
		FileStatus f=null;
		try {
			fs = getFileSystem();
			Path dir = new Path(webpath);
			f= fs.getFileStatus(dir);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}      
		Calendar cal = Calendar.getInstance();  
		String timechange="";
		//��ȡ�ļ�ʱ��
		long time = f.getModificationTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		//ת���ļ�����޸�ʱ��ĸ�ʽ
		cal.setTimeInMillis(time);    

		timechange = formatter.format(cal.getTime());
		return timechange;
	}
	//�Ƿ���׷�ӵ���ʽ�����ļ�
	public void outFileToLocal(String txt,String outfile,boolean isappend) throws IOException
	{
		File fi=new File(outfile);
		FileOutputStream fop=new FileOutputStream(fi,isappend);
		OutputStreamWriter ops=new OutputStreamWriter(fop,"UTF-8");
		ops.append(txt);
		ops.close();
		fop.close();
	}
	//�ж��ļ��Ƿ����
	public boolean judgeFileExists(String path) {

		File file=new File(path);
		if (file.exists()) {
			return true;
		} else {
			return false;
		}

	}

	//�����ļ�ʱ�ж��ļ�����
	public boolean judeFileExistsNoDepend(File file) {

		if (file.exists()) {
			return true;
		} else {
			return false;
		}

	}

	//��ȡ�ļ�ÿһ�е����ݲ��ҷ����ַ���������
	public List<String> inputFile(String webpath) throws IOException
	{

		List<String> strlist=new ArrayList<String>();
		InputStream b =getFileSystem().open(new Path(webpath));
		InputStreamReader c=new InputStreamReader(b,"UTF-8");


		{
			BufferedReader bufr =new BufferedReader(c);
			String line = null;
			while((line = bufr.readLine())!=null){
				//line��ÿһ�е�����
				strlist.add(line);
			}
			bufr.close();
		}
		c.close();
		b.close();
		return strlist;
	}

	//�Ƿ���׷�ӵ���ʽ�����ļ�
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

	//����ļ�����������
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


	//ɾ���ļ��еĵ�n������
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


	//���ַ����������м����δ������ļ���
	public void outFileByStringList(List<String> strlist,String outfile,String lineDecorate)
	{
		try {
			outFile(new StringHandle().StringListIntoString(strlist, lineDecorate),outfile,false);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}

	//���ַ��������Կո���м����δ������ļ���
	public void outFileByStringListList(List<List<String>> strlist,String outfile,String lineDecorate)
	{
		try {
			StringHandle sh=new StringHandle();
			outFile(sh.StringListIntoString(sh.StringListListIntoStringList(strlist," "),lineDecorate ),outfile,false);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}

	//����ļ����޸�ʱ��
	public String getModifiedTime(String path){  
		File f = new File(path);              
		Calendar cal = Calendar.getInstance();  
		String timechange="";
		//��ȡ�ļ�ʱ��
		long time = f.lastModified();  
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		//ת���ļ�����޸�ʱ��ĸ�ʽ
		cal.setTimeInMillis(time);    

		timechange = formatter.format(cal.getTime());
		return timechange;
		//������޸�ʱ��[2]    2009-08-17 10:32:38  
	}  

	//�ж��ļ��Ƿ�Ϊ��
	public boolean fileIsEmpty(String path)
	{
		File fi=new File(path);

		if(fi.length()==0||!fi.exists())
			return true;
		else return false;
	}

	//����ļ�Ŀ¼�������ϢתΪ�ַ���������������
	public List<List<String>> getInfosList(String path)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
		List<List<String>> objinfo=sh.StringSplitByExpToStringList(objline, " ");;
		return objinfo;
	}

	//����ļ�Ŀ¼�������ϢתΪ��Ӧ�������
	public <T> List<T> getInfosToTlist(String path,String []nameNlist,Class<T> clazz)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
		List<T> objinfo=sh.StringSplitByExpToTList(objline, " ",nameNlist,clazz);;
		return objinfo;
	}

	//����ļ�Ŀ¼���������ϢתΪ��Ӧ�������(�Զ���)
	public <T> List<T> getInfosToTlist(String path,Class<T> clazz)
	{
		StringHandle sh=new StringHandle();
		List<String> objline=null;
		try {
			objline=inputFile(path);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
		List<T> objinfo=sh.StringSplitByExpToTList(objline, " ",clazz);;
		return objinfo;
	}

	//����Ӧ������������������ݴ����Ӧ�ļ���
	public <T> void outputFileByTlist(List<T> obj,Class<?> clazz,String path,boolean isAppend)
	{
		EntityToString ets=new EntityToString();
		StringHandle sh=new StringHandle();
		try {
			if(fileIsEmpty(path))
				outFile(sh.StringListIntoString(ets.getStringList(obj, clazz),"\r\n"),path,isAppend);
			else outFile("\r\n"+sh.StringListIntoString(ets.getStringList(obj, clazz),"\r\n"),path,isAppend);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}
	public <T> void outputFileByTlist(List<T> obj,String path,boolean isAppend)
	{
		outputFileByTlist(obj,obj.get(0).getClass(),path,isAppend);
	}


	//����Ӧ����������ӵ���Ӧ�ļ���
	public <T> void outputFileByT(T obj,Class<?> clazz,String path,boolean isAppend)
	{
		EntityToString ets=new EntityToString();
		try {
			if(fileIsEmpty(path))
				outFile(ets.getString(obj, clazz),path,isAppend);
			else outFile("\r\n"+ets.getString(obj, clazz),path,isAppend);
		} catch (IOException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}

	public <T> void outputFileByT(T obj,String path,boolean isAppend)
	{
		outputFileByT(obj,obj.getClass(),path,isAppend);
	}

}