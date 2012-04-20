import org.junit.Test;
import ru.mltny.vfs.Settings;
import ru.mltny.vfs.VFSTools;
import ru.mltny.vfs.VFSToolsImpl;
import ru.mltny.vfs.units.Cluster;
import ru.mltny.vfs.units.Node;
import sun.reflect.misc.FieldUtil;

import java.io.*;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/4/12
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */


public class TestArchive {

    @Test
    public void test() {
        try {
            VFSToolsImpl tools = new VFSToolsImpl("/Users/maloletniy/Test/container.txt");

            //Создаем контейнер
            tools.createContainer();

            long time = System.currentTimeMillis();
            Node n = tools.getNodeByPath(0);
            System.out.println("find node:" + (System.currentTimeMillis() - time));

            time = System.currentTimeMillis();
            tools.create('f', "rivet.zip", n);
            System.out.println("create node:" + (System.currentTimeMillis() - time));

            /* time = System.currentTimeMillis();
                        tools.create('f', "readme.txt", n);
                        System.out.println("create node:" + (System.currentTimeMillis() - time));
            */
            n = tools.getNodeByPath(0);
            System.out.println(n);

            byte[] testData = new byte[1025];
            byte[] testData2 = new byte[1023];

            testData[0] = 100;
            testData[1024] = 101;
            testData2[0] = 102;
            testData2[1022] = 103;


            for (Long link : n.getLink()) {
                if (link > 0) {
                    Node node = tools.getNodeByPath(link);

                    time = System.currentTimeMillis();
                    tools.write(node, testData);
                    System.out.println("write node data:" + (System.currentTimeMillis() - time));
                }
            }

            for (Long link : n.getLink()) {
                if (link > 0) {
                    Node node = tools.getNodeByPath(link);
                    System.out.println(node);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
