package com.taodatarobot.engine.py4j.hdfs;

import com.taodatarobot.engine.py4j.util.CONSTANT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HDFSUtil {
    static{
        System.setProperty("HADOOP_USER_NAME", CONSTANT.HDFS_USER);
    }

    /**
     * 将本地文件(filePath)上传到HDFS服务器的指定路径(dst)
     * @param filePath
     * @param dst
     * @throws Exception
     */
    public static void uploadToHDFS(String filePath,String dst) throws IOException {
        //创建一个文件系统
        FileSystem fs = null;
        try{
            fs = FileSystem.get(getConfig(null));
            Path srcPath = new Path(filePath);
            Path dstPath = new Path(dst);
            if(fs.exists(dstPath)){
                fs.delete(dstPath, true);
            }
            fs.copyFromLocalFile(false, srcPath, dstPath);
        } catch (IOException e) {
            throw e;
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }

    public static void delete(String path) throws IOException {
        //创建一个文件系统
        FileSystem fs = null;
        try{
            fs = FileSystem.get(getConfig(null));
            Path filePath = new Path(path);
            fs.delete(filePath, true);
        } catch (IOException e) {
            throw e;
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }

    private static Configuration getConfig(String path){
        Configuration conf = new Configuration(true);
        String hdfs_url = path==null? CONSTANT.HDFS_URL:"hdfs://"+ path.split("/")[2];
        conf.set("fs.default.name", hdfs_url);
        return conf;
    }

    public static void readAndAppendFile(String srcPath, Iterable<String> append, String targetPath) throws IOException{
        //创建一个文件系统
        FileSystem fs = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        try{
            fs = FileSystem.get(getConfig(srcPath));
            Path src = new Path(srcPath);
            Path target = new Path(targetPath);
            if(fs.exists(target)){
                fs.delete(target, true);
            }
            br = new BufferedReader(new InputStreamReader(fs.open(src)));
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(target, true)));
            String line;
            while((line = br.readLine())!=null){
                bw.write(line);
                bw.newLine();
            }
            br.close();
            for (String add : append){
                bw.write(add);
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            throw e;
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }

    public static void downLoadLog(String appId, String user, String dst) throws IOException {
        String logDir = CONSTANT.YARN_LOG_DIRECTORY + "/" + user + "/logs/" + appId;
        // 查看下面有什么文件
        //创建一个文件系统
        FileSystem fs = null;
        try{
            fs = FileSystem.get(HDFSUtil.getConfig(null));
            Path logDirPath = new Path(logDir);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(logDirPath, false);
            while(files.hasNext()){
                LocatedFileStatus status = files.next();
                Path logPath = status.getPath();
                fs.copyToLocalFile(false,logPath, new Path(dst),true);
                break;
            }
        } catch (FileNotFoundException e) {
            throw new IOException("APPLICATION_LOG_NOT_FOUND");
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }


    public static void download(String from, String to) throws IOException {
        FileSystem fs = null;
        try{
            fs = FileSystem.get(HDFSUtil.getConfig(null));
            Path fromPath = new Path(from);
            fs.copyToLocalFile(false,fromPath, new Path(to),true);
        } catch (FileNotFoundException e) {
            throw new IOException("FILE_NOT_FOUND");
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }


    public static void deleteDirectory(String path) throws IOException{
        FileSystem fs = null;
        try{
            fs = FileSystem.get(HDFSUtil.getConfig(null));
            Path p = new Path(path);
            fs.delete(p, true);
        } catch (FileNotFoundException e) {
            throw new IOException("APPLICATION_LOG_NOT_FOUND");
        }finally {
            if(fs!=null){
                fs.close();
            }
        }
    }
}
