/*
 * Copyright (c) 2014 杭州端点网络科技有限公司
 */

package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CopyFile {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);


        //本地文件
        Path src = new Path("");

        //HDFS为止
        Path dst = new Path("/");


        hdfs.copyFromLocalFile(src, dst);

        System.out.println("Upload to" + conf.get("fs.default.name"));

        FileStatus files[] = hdfs.listStatus(dst);

        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }

    }

}
