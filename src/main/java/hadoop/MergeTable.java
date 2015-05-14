/*
 * Copyright (c) 2014 杭州端点网络科技有限公司
 */

package hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.out;

public class MergeTable {


    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", args[0]);
        String rootDir = args[1];

        FileSystem fs = FileSystem.get(conf);

        Path root = new Path(rootDir);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(root, true);

        LinkedList<Bulk> bulks = Lists.newLinkedList();

        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();
            if(bulks.size()==0)
                bulks.add(Bulk.build());
            if(bulks.getLast().size + status.getLen() > status.getBlockSize())
                bulks.add(Bulk.build());
            bulks.getLast().paths.add(status.getPath());
            bulks.getLast().size += status.getLen();
        }

        for(Bulk bulk: bulks){
            if(bulk.paths.size()<=1) continue;
            String path = bulk.paths.get(0).toString();
            String parent = path.substring(0, path.lastIndexOf('/'));
            Path destFilePath = new Path(parent+"/part-merged-"+bulk.paths.size()+"-"+new SimpleDateFormat("YYYYMMddHHmmss").format(new Date()));
            FSDataOutputStream destOS = fs.create(destFilePath);
            for(Path p: bulk.paths) {
                out.printf("merge %s \nto %s\n\n", p, destFilePath);
                FSDataInputStream srcIS = fs.open(p);
                int len;
                byte[] fileContent = new byte[1024];
                while((len=srcIS.read(fileContent)) > 0)
                    destOS.write(fileContent, 0, len);
                srcIS.close();
            }
            destOS.close();

            for(Path p: bulk.paths)
                fs.delete(p, false);

        }

    }


    public static class Bulk{
        public static Bulk build(){
            return new Bulk();
        }
        int size;
        List<Path> paths = Lists.newArrayList();
    }


}
