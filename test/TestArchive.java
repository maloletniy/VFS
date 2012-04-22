import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import ru.mltny.vfs.Settings;
import ru.mltny.vfs.VFSToolsImpl;
import ru.mltny.vfs.units.Cluster;
import ru.mltny.vfs.units.Node;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/4/12
 * Time: 5:14 PM
 * Test
 */


public class TestArchive {

    private String path = "/Users/maloletniy/Test/container.txt";
    VFSToolsImpl tools;
    private final Logger LOG = Logger.getLogger(TestArchive.class);

    @Test
    public void test() {
        try {
            VFSToolsImpl tools = new VFSToolsImpl(path);

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
            testData[0] = 100;
            testData[1024] = 101;


            //todo maloletniy тесты для переименования\перемещения и append

            for (Long link : n.getLink()) {
                if (link > 0) {
                    Node node = tools.getNodeByPath(link);

                    time = System.currentTimeMillis();
                    tools.write(node, testData);
                    LOG.debug("write node data:" + (System.currentTimeMillis() - time));
//                    System.out.println("write node data:" + (System.currentTimeMillis() - time));
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

    @Test
    public void testCreateContainer() throws IOException {

        tools = new VFSToolsImpl(path);

        long time = System.currentTimeMillis();
        tools.createContainer();
        LOG.info("Create container:" + (System.currentTimeMillis() - time));

        Node node = tools.getNodeByPath(0);
        LOG.info(node);
        if (node.getName()[0] != '.')
            assert false;

        Cluster cluster = tools.getClusterByPath(Settings.CLUSTER_FIRST_ADDRESS);
        LOG.info(cluster);
        if (cluster.getSize() != 0 || cluster.getLink() != -1)
            assert false;
    }

}
