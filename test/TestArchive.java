import org.apache.log4j.Logger;
import org.junit.Test;
import ru.mltny.vfs.Settings;
import ru.mltny.vfs.VFSToolsImpl;
import ru.mltny.vfs.units.Cluster;
import ru.mltny.vfs.units.Node;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/4/12
 * Time: 5:14 PM
 * Test
 */


public class TestArchive {

    private static VFSToolsImpl tools;
    private final Logger LOG = Logger.getLogger(TestArchive.class);

    private final String TEST_DIR = "./Container_Test";
    private final String CONT_NAME = "containter.txt";
    private final String CONT_PATH = TEST_DIR + "/" + CONT_NAME;

    private static Node prevStepNode = null;

    @Test
    public void createTestDir() throws Exception {
        File f = new File(TEST_DIR);
        if (!f.exists() && !f.mkdirs()) {
            throw new Exception("TeastDir create fail");
        }
    }

    //Тест на создание контейнера. Создаем контейнер, проверяем, что первая нода это всегда корень
    // и что первый кластер имеетразмер 0 и никуда не ссылается

    @Test
    public void testCreateContainer() throws Exception {

//        final String path = "./container.txt";

        tools = new VFSToolsImpl(CONT_PATH);

        long time = System.currentTimeMillis();
        tools.createContainer();
        LOG.info("Create container:" + (System.currentTimeMillis() - time));

        Node node = tools.getNodeByPath(0);
        LOG.info(node);
        if (node.getName()[0] != '.')
            throw new Exception("First node is not root");

        Cluster cluster = tools.getClusterByPath(Settings.CLUSTER_FIRST_ADDRESS);
        LOG.info(cluster);
        if (cluster.getSize() != 0 || cluster.getLink() != -1)
            throw new Exception("First cluster does not contain right data");

        prevStepNode = node;
    }

    //Проверяем что создали файл, записали в него данные, прочитав данные, убедились, что они верны записанным
    @Test
    public void createDirFileWriteRead() throws Exception {
//        testCreateContainer();
        Node root = prevStepNode;


        //Создаем директорию Test в корне
        LOG.info("Creates new dir 'Test' in '.'");

        long time = System.currentTimeMillis();
        Node testDir = tools.create('d', "Test", root);
        LOG.info("Create node: " + (System.currentTimeMillis() - time));


        //Проверяем ссылку родителя
        Arrays.sort(root.getLink());
        int dirLinkIndex = Arrays.binarySearch(root.getLink(), testDir.getAddress());
        if (dirLinkIndex < 0) {
            throw new Exception("Root directory does not contain link to Test dir");
        }
        LOG.info(root);
        LOG.info(testDir);

        //Создаем файл test.txt в директории Test
        LOG.info("Creates new file 'text.txt' in Test");
        time = System.currentTimeMillis();
        Node testFile = tools.create('f', "text.txt", testDir);
        LOG.info("Create node: " + (System.currentTimeMillis() - time));


        //Проверяем ссылку на него в директории
        Arrays.sort(testDir.getLink());
        dirLinkIndex = Arrays.binarySearch(root.getLink(), testDir.getAddress());
        if (dirLinkIndex < 0) {
            throw new Exception("Directory does not contain link t file");
        }

        LOG.info(testDir);
        LOG.info(testFile);

        //Пишем данные в файл
        byte[] data = new byte[Settings.CLUSTER_SIZE_BYTES + 1];
        data[0] = 100;
        data[Settings.CLUSTER_SIZE_BYTES - 1] = 101;
        data[Settings.CLUSTER_SIZE_BYTES] = 99;

        LOG.info("writes " + data.length + " bytes of data to text.txt");
        time = System.currentTimeMillis();
        tools.write(testFile, data);
        LOG.info("Writes data: " + (System.currentTimeMillis() - time));

        LOG.info(testFile);
        long nextClusterLink = testFile.getLink()[0];
        while (nextClusterLink != -1) {
            Cluster c = tools.getClusterByPath(nextClusterLink);
            nextClusterLink = c.getLink();
            LOG.info(c);
        }

        //Читаем данные
        byte[] dataRead = tools.read(testFile);

        //Проверям что они равны записанным
        if (!Arrays.equals(data, dataRead)) {
            throw new Exception("writed and readed file data are not equals");
        }
        prevStepNode = testFile;
    }

    //Добавляет к существующему файлу такие же данные и проверяем что обе части равные
    @Test
    public void AppendData() throws Exception {
//        createDirFileWriteRead();
        Node testFile = prevStepNode;

        byte[] testFileData = tools.read(testFile);

        //Добавляем те же самые денные в файл
        LOG.info("Appends data to text.txt");
        long time = System.currentTimeMillis();
        tools.append(testFile, testFileData);
        LOG.info("Append data: " + (System.currentTimeMillis() - time));

        LOG.info(testFile);

        //Печатаем инфу о кластерах а заодно проеряем занимают ли новые денные 3 кластера
        long nextClusterLink = testFile.getLink()[0];
        int clusterCount = 0;
        while (nextClusterLink != -1) {
            Cluster c = tools.getClusterByPath(nextClusterLink);
            nextClusterLink = c.getLink();
            LOG.info(c);
            clusterCount++;
        }
        if (clusterCount != 3) {
            throw new Exception("New file cluster size is not 3");
        }
        // Опять читаем данные и режем их посередине и сравниваем части, что они равны
        testFileData = tools.read(testFile);

        byte[] data1 = Arrays.copyOfRange(testFileData, 0, Settings.CLUSTER_SIZE_BYTES);
        byte[] data2 = Arrays.copyOfRange(testFileData, Settings.CLUSTER_SIZE_BYTES + 1, (Settings.CLUSTER_SIZE_BYTES + 1) * 2 - 1);
        if (!Arrays.equals(data1, data2)) {
            throw new Exception("Appended data is not equal old data");
        }

    }

    //Переименовывыем файл, перемещаем его в корень, удаляем папку Test, удаляем файл
    @Test
    public void RenameMoveDelete() throws Exception {
//        createDirFileWriteRead();

        Node root = tools.getNodeByPath(0);
        Arrays.sort(root.getLink());
        long link = root.getLink()[Settings.MAX_DIR_COUNT - 1];

        Node testFile = prevStepNode;
        Node testDir = tools.getNodeByPath(link);

        LOG.info("Renaming file to text2.txt");

        long time = System.currentTimeMillis();
        tools.rename(testFile, testDir, "text2.txt");
        LOG.info("Renamed: " + (System.currentTimeMillis() - time));


        testFile = tools.getNodeByPath(testFile.getAddress());
        if (!new String(testFile.getName()).trim().equals("text2.txt")) {
            throw new Exception("Rename error");
        }
        //Перемещаем файл в корень
        LOG.info("Moving test file");

        time = System.currentTimeMillis();
        tools.move(testFile, testDir, root);
        LOG.info("Move file: " + (System.currentTimeMillis() - time));

        root = tools.getNodeByPath(0);
        testDir = tools.getNodeByPath(testDir.getAddress());

        //Проверяем что файла в тест дире больше нет
        if (!Arrays.equals(testDir.getLink(), new long[Settings.MAX_DIR_COUNT])) {
            throw new Exception("Test directory not empty");
        }

        //Проверяем что файл появился в корне
        Arrays.sort(root.getLink());
        if (Arrays.binarySearch(root.getLink(), testFile.getAddress()) < 0) {
            throw new Exception("File not appeared in root directory");
        }

        //Удаляем тестовую директорию
        LOG.info("Removing Test directory");

        time = System.currentTimeMillis();
        tools.delete(testDir, root);
        LOG.info("Remove directory: " + (System.currentTimeMillis() - time));

        root = tools.getNodeByPath(0);
        Arrays.sort(root.getLink());
        if (Arrays.binarySearch(root.getLink(), testDir.getAddress()) > 0) {
            throw new Exception("Test directory is not removed");
        }

        //Удаляем тестовую директорию
        LOG.info("Removing test file");

        time = System.currentTimeMillis();
        tools.delete(testFile, root);
        LOG.info("Remove file: " + (System.currentTimeMillis() - time));

        root = tools.getNodeByPath(0);
        Arrays.sort(root.getLink());
        if (Arrays.binarySearch(root.getLink(), testFile.getAddress()) > 0) {
            throw new Exception("Test file is not removed");
        }
        LOG.info(root);


    }

    @Test
    public void packDirectoryToContainer() throws Exception {
        //запаковываем директорию с проектом
        String path = ".";
        File f = new File(path);
        if (!f.exists() && !f.mkdir()) {
            throw new Exception("Create Test folder error");
        }

        testCreateContainer();
        Node root = tools.getNodeByPath(0);
        addDirectory(f, root);
        root = tools.getNodeByPath(0);
        LOG.info(root);

    }

    private void addFile(Node node, File file) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FileInputStream fos = new FileInputStream(file);
        byte[] buf = new byte[1024];
        int len;
        LOG.info("writing file  " + file.getName() + " size = " + file.length());
        while ((len = fos.read(buf)) > 0) {
            /*if (len < 1024) {
                tools.append(node, Arrays.copyOfRange(buf, 0, len));
            } else {
                tools.append(node, buf);
            }*/
            out.write(buf, 0, len);
        }
        tools.write(node, out.toByteArray());
    }

    private void addDirectory(File dir, Node parent) throws Exception {
        if (dir.getName().equals(".") || dir.getName().equals(".idea") ||
                (!dir.getPath().equals(TEST_DIR) && !dir.getName().startsWith(".")))

            for (File file : dir.listFiles()) {
                char type = 'f';
                if (file.isDirectory()) {
                    type = 'd';
                }
                //skip files and dirs here
                Node fileNode = tools.create(type, file.getName(), parent);
                if (file.isFile()) {
                    addFile(fileNode, file);
                }
                if (file.isDirectory()) {
                    addDirectory(file, fileNode);
                }
                LOG.info(fileNode);
            }
    }


    @Test
    public void unpackRootNodeToDirectory() throws Exception {
//        final String path = "./container.txt";
        tools = new VFSToolsImpl(CONT_PATH);

        Node root = tools.getNodeByPath(0);

        //Распаковываем в папку теста
        File f = new File(TEST_DIR + "/UnpackTest");
        if (!f.exists() && !f.mkdir()) {
            throw new Exception("Create directory " + f.getName() + " failed");
        }
        createDirectory(root, f);

    }

    public void saveFile(Node file, File dir) throws Exception {
        File f = new File(dir, String.valueOf(file.getName()).trim());
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(tools.read(file));
        fos.close();
    }

    public void createDirectory(Node node, File dir) throws Exception {
        for (long l : node.getLink()) {
            if (l == 0) continue;
            Node file = tools.getNodeByPath(l);
            LOG.info(String.valueOf(file.getName()).trim());
            if (file.getType() == 'd') {
                File subDir = new File(dir, String.valueOf(file.getName()).trim());
                if (!subDir.exists() && !subDir.mkdir()) {
                    throw new Exception("Create directory " + subDir.getName() + " failed");
                }
                createDirectory(file, subDir);
            } else if (file.getType() == 'f') {
                saveFile(file, dir);
            }
        }
    }


}
