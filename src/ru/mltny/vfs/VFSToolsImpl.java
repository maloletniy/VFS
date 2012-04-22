package ru.mltny.vfs;

import org.apache.log4j.Logger;
import ru.mltny.vfs.units.Cluster;
import ru.mltny.vfs.units.Node;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/15/12
 * Time: 6:14 PM
 * implementation of vfsTools
 */
public class VFSToolsImpl implements VFSTools {

    private RandomAccessFile file;
    private final Logger LOG = Logger.getLogger(VFSToolsImpl.class);

    //состояние кластеров
    //Получаем на 10гб(20971520 clusters) 85мб занимает только хранение состояний кластеров
    //todo malolentiy тут облажался, надо хранить адреса а не индексы, иначе не покрывает весь long
    private int[] emptyClusters = new int[Settings.CLUSTER_COUNT];
    //Состояние нодов
    private long[] emptyNodes = new long[Settings.NODE_COUNT];

    public VFSToolsImpl(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");

        //Если контейнер не пуст читаем инфу по кластерам
        if (file.length() > 0) {
            updateContainerInfo();
        }
    }

    /**
     * reads info about clusters status, etc...
     *
     * @throws IOException if any container problem
     */
    private void updateContainerInfo() throws IOException {
        long time1 = System.currentTimeMillis();
        //todo maloletniy из за того что начало от 0, мы потеряли один индекс, надо переходить на адреса в виде long
        for (int i = 0; i < emptyClusters.length; i++) {
            file.seek(Settings.CLUSTER_FIRST_ADDRESS + i * Cluster.getObjectSize());
            //Если кластар занят то значение его = 0  - а надо бы было хотя бы -1 сделать
            if (file.readInt() > 0) {
                emptyClusters[i] = 0;
            } else {
                //сли кластре свободен то хранием его индекc
                emptyClusters[i] = i;
            }
        }

        for (int i = 1; i < emptyNodes.length; i++) {
            file.seek(i * Node.getObjectSize());
            //Если нода заняата(тип d или f ) то значение ее = 0
            if (file.readChar() != '-') {
                emptyNodes[i] = 0;
            } else {
                //если нода свободна то хранием ее адрес
                emptyNodes[i] = i * Node.getObjectSize();
            }
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Update container: " + (System.currentTimeMillis() - time1));
        }
    }

    private int getEmptyClusterIndex() throws Exception {
        Arrays.sort(emptyClusters);
        int address = emptyClusters[emptyClusters.length - 1];
        if (address == 0) {
            throw new Exception("All clusters are full");
        }
        emptyClusters[emptyClusters.length - 1] = 0;
        return address;

    }

    private long getEmptyNodeAddress() throws Exception {
        Arrays.sort(emptyNodes);
        long address = emptyNodes[emptyNodes.length - 1];
        if (address == 0) {
            throw new Exception("All nodes are full");
        }
        emptyNodes[emptyNodes.length - 1] = 0;
        return address;
    }

    /**
     * Fills container with nodes and clusters
     *
     * @throws IOException if any container problem
     */
    public void createContainer() throws IOException {

        //Создаем первичную ноду
        //Пишем размер кластера в заголовок
        Node node = new Node();
        node.setSize(0);
        node.setType('d');
        node.setName(".");
        node.writeNode(file);

        long time1 = System.currentTimeMillis();
        //Заполняем все ноды пустышками
        for (int i = 1; i < Settings.NODE_COUNT; i++) {
            if (LOG.isInfoEnabled() && i % (Settings.NODE_COUNT / 10) == 0) {
                LOG.info(i * 100 / Settings.NODE_COUNT + "%..");
            }
            Node node2 = new Node();
            node2.setSize(0);
            node2.setType('-');
            node2.setAddress(i * Node.getObjectSize());
            node2.writeNode(file);
        }

        LOG.info("Create nodes: " + (System.currentTimeMillis() - time1));

        time1 = System.currentTimeMillis();
        //заполняем инфу о кластерах
        for (int i = 0; i < Settings.CLUSTER_COUNT; i++) {
            if (LOG.isInfoEnabled() && i % (Settings.CLUSTER_COUNT / 10) == 0) {
                LOG.info(i * 100 / Settings.CLUSTER_COUNT + "%..");
            }
            Cluster cluster = new Cluster(-1, 0, Settings.CLUSTER_FIRST_ADDRESS + i * Cluster.getObjectSize());
            cluster.writeClusterInfo(file);
        }
        LOG.info("Create clusters: " + (System.currentTimeMillis() - time1));

        updateContainerInfo();
    }

    /**
     * Create node of special type with link to parent node
     *
     * @param type   'f' - file , 'd'-folder, '-' - free node
     * @param name   name of node(will autocut to 16chars)
     * @param parent node which will store link to created node
     * @return Node of created file
     * @throws IOException
     */
    public Node create(char type, String name, Node parent) throws Exception {
        //Ищем пустую ноду
        long freeNodePointer = getEmptyNodeAddress();

        Node node = new Node();
        node.setName(name);
        node.setType(type);
        node.setSize(0);

        node.setAddress(freeNodePointer);
        node.writeNode(file);

        //Ищем свободное место в родителе куда записать указатель (сортируем и берем первый элемент)
        Arrays.sort(parent.getLink());

        if (parent.getLink()[0] != 0) {
            throw new Exception("Directory limit error");
        }

        parent.getLink()[0] = node.getAddress();

        //Обновляем скисок ссылок родителя в контейнере
        parent.updateLinks(file);

        return node;
    }

    public void write(Node node, byte[] bytes) throws Exception {
        if (node.getType() != 'f') {
            throw new Exception("cannot write - not a file: \n" + node);
        }

        int oldClusterCount = (int) Math.ceil((double) node.getSize() / Settings.CLUSTER_SIZE_BYTES);
        int newClusterCount = (int) Math.ceil((double) bytes.length / Settings.CLUSTER_SIZE_BYTES);
        long firstClusterLink;

        //Если новый размер меньше старого и придется обновлять список свободных кластеров
        // то сортируем его чтобы в начале были занятые кластеры
        if (oldClusterCount > newClusterCount) {
            Arrays.sort(emptyClusters);
        }

        //Смотрим есть ли уже ссылки на кластера
        if (node.getLink()[0] != 0) {
            firstClusterLink = node.getLink()[0];

            //бежим по существующим кластерам
            //Пишем пока не закончатся существующие кластеры или данные
            long nextClusterLink = firstClusterLink;
            for (int i = 0; i < newClusterCount; i++) {
                Cluster cluster = Cluster.getClusterAtPoint(file, nextClusterLink);

                //Работа со ссылкой
                //Если требуются еще кластеры
                if (i < newClusterCount - 1) {
                    //Есть ли ссылка на след кластер кторый уже используется
                    if (cluster.getLink() != -1) {
                        nextClusterLink = cluster.getLink();
                    } else {
                        nextClusterLink = Settings.CLUSTER_FIRST_ADDRESS +
                                getEmptyClusterIndex() * Cluster.getObjectSize();
                    }

                } else {
                    //Если есть ссылки на ранее используемые кластеры - затираем их
                    // и добавляем их индексы в список пустых кластеров
                    if (cluster.getLink() != -1) {
                        long link = cluster.getLink();
                        int fullClusterIndex = 0;
                        while (link != -1) {
                            //Ищем слд связаный кластер
                            Cluster c = Cluster.getClusterAtPoint(file, link);
                            link = c.getLink();
                            int index = c.getClusterIndex();

                            //Обновляем список свободных кластеров
                            emptyClusters[fullClusterIndex] = index;
                            fullClusterIndex++;

                            c.setSize(0);
                            c.setLink(-1);
                            c.writeClusterInfo(file);
                        }
                    }
                    nextClusterLink = -1;
                }
                cluster.setLink(nextClusterLink);

                //Размер данных которые записаны в кластер
                //Пишем сколько места от кластера мы заняли (полный кластер или остаток )
                long size = bytes.length - i * Settings.CLUSTER_SIZE_BYTES;
                if (size > Settings.CLUSTER_SIZE_BYTES) {
                    cluster.setSize(Settings.CLUSTER_SIZE_BYTES);
                } else {
                    cluster.setSize((int) size);
                }
                cluster.writeClusterInfo(file);

                //Пишем данные в кластер
                cluster.writeClusterData(file, bytes, i * Settings.CLUSTER_SIZE_BYTES);
            }
        } else {

            //Ищем свободный кластер
            int nextClusterIndex = getEmptyClusterIndex();

            //Ссылка на первый кластер с данными(для того чтобы нода знала начало)
            firstClusterLink = Settings.CLUSTER_FIRST_ADDRESS + nextClusterIndex * Cluster.getObjectSize();

            for (int i = 0; i < newClusterCount; i++) {

                Cluster cluster = new Cluster(Settings.CLUSTER_FIRST_ADDRESS + nextClusterIndex * Cluster.getObjectSize());

                //Если этого кластера не хватает для записи данных ищем след свободный и делаем ссылку на него
                if (i < newClusterCount - 1) {
                    //Ищем кластер
                    nextClusterIndex = getEmptyClusterIndex();
                    //Ставим ссылку в текущем кластере
                    cluster.setLink(Settings.CLUSTER_FIRST_ADDRESS + nextClusterIndex * Cluster.getObjectSize());
                } else {
                    cluster.setLink(-1);
                }
                //Размер данных которые записаны в кластер
                //Пишем сколько места от кластера мы заняли (полный кластер или остаток )
                long size = bytes.length - i * Settings.CLUSTER_SIZE_BYTES;
                if (size > Settings.CLUSTER_SIZE_BYTES) {
                    cluster.setSize(Settings.CLUSTER_SIZE_BYTES);
                } else {
                    cluster.setSize((int) size);
                }
                cluster.writeClusterInfo(file);

                //Пишем данные в свободный кластер
                cluster.writeClusterData(file, bytes, i * Settings.CLUSTER_SIZE_BYTES);

            }
        }
        //В ноде делаем ссылку на первый кластер в который мы пишем инфу и обновляем размер
        node.getLink()[0] = firstClusterLink;
        node.updateLinks(file);
        node.setSize(bytes.length);
        node.updateSize(file);
    }

    /**
     * Reads file from clusters until it reach file size
     *
     * @param node - file node of readable file
     * @return byte represent of file
     * @throws IOException
     */
    public byte[] read(Node node) throws Exception {
        byte[] result = new byte[node.getSize()];

        //вытаскиваем ссылку на первый кластер
        Cluster cluster = Cluster.getClusterAtPoint(file, node.getLink()[0]);
        cluster.getClusterData(file, result, 0);
        int offset = cluster.getSize();

        while (cluster.getLink() != -1) {
            cluster = Cluster.getClusterAtPoint(file, cluster.getLink());
            cluster.getClusterData(file, result, offset);
            offset += cluster.getSize();
        }

        return result;
    }

    /**
     * Appends bytes to last cluster
     *
     * @param node  node to append data
     * @param bytes data to append
     * @throws Exception any container exception
     */
    public void append(Node node, byte[] bytes) throws Exception {
        if (node.getType() != 'f') {
            throw new Exception("Node it not file ");
        }
        long nextClusterAddress = node.getLink()[0];
        Cluster c = null;
        int offset = 0;

        //Ищем последний кластер в цепочке
        while (nextClusterAddress != -1) {
            c = Cluster.getClusterAtPoint(file, nextClusterAddress);
            nextClusterAddress = c.getLink();
        }
        if (c == null) {
            throw new Exception("Cluster calculate error");
        }
        //Смотрим, можем ли мы дописать в послед кластер
        if (c.getSize() < Settings.CLUSTER_SIZE_BYTES) {
            offset += c.appendClusterData(file, bytes, offset);
            //Обновляем инфу о размере записанных данных
            c.setSize(c.getSize() + offset);
        }

        //Если еще остались данные после допиcи в кластер
        if (offset < bytes.length) {
            // берем новые кластера, и пишем в них до конца данных, делаем ссылки на след кластера и пишем размер
            nextClusterAddress = Settings.CLUSTER_FIRST_ADDRESS + getEmptyClusterIndex() * Cluster.getObjectSize();
            //Обновляем в последем кластере ссылку на след
            c.setLink(nextClusterAddress);

            while (offset < bytes.length) {
                Cluster cluster = new Cluster(nextClusterAddress);

                //Если этого кластера не достаточно для записи всех данных то нужно селать ссылку на след
                if ((offset + Settings.CLUSTER_SIZE_BYTES) < bytes.length) {
                    nextClusterAddress = Settings.CLUSTER_FIRST_ADDRESS + getEmptyClusterIndex() * Cluster.getObjectSize();
                    cluster.setLink(nextClusterAddress);
                } else {
                    cluster.setLink(-1);
                }
                //пишем данные и обновляем размер записанных данных
                int size = cluster.writeClusterData(file, bytes, offset);
                offset += size;
                cluster.setSize(size);

                //Обновляем информацию о кластере
                cluster.writeClusterInfo(file);
            }
        }
        c.writeClusterInfo(file);

        //Обновляем рамер записанных данных
        node.setSize(node.getSize() + bytes.length);
        node.updateSize(file);
    }

    /**
     * Renames file or directory
     *
     * @param node Node which name will be renamed
     * @param name new name of node
     * @throws Exception if any container problem
     */
    public void rename(Node node, Node parent, String name) throws Exception {
        if (node.getAddress() == 0) {
            throw new Exception("Cannot rename root");
        }
        char[] charName = name.toCharArray();
        boolean inDir = false;
        //проверяем уникальность имен в родителе
        for (long l : parent.getLink()) {

            if (Arrays.equals(Node.getNodeAtPoint(file, l).getName(), charName)) {
                throw new Exception("duplicate file name");
            }
            //заодно проверяем что нода принадлежит родителю
            if (l == node.getAddress()) {
                inDir = true;
            }
        }
        if (!inDir) {
            throw new Exception("Parent node doesn't contains renaming node");
        }
        node.setName(name);
        node.updateName(file);
    }

    /**
     * Removes node from parent node and clears clusters
     *
     * @param node   which will be deleted
     * @param parent of deleted node
     * @throws Exception if any container or logic problem
     */
    public void delete(Node node, Node parent) throws Exception {
        if (node.getAddress() == 0) {
            throw new Exception("Cannot delete root");
        }
        //Если файл то бежим по всем кластерам, отмечаем размер 0, ссылку -1 и помещаем индекс в список пустых кластеров,
        //а также удаляем из родительской ноды ссылку
        if (node.getType() == 'f') {
            //Вытаскиваем ссылку на первый кластер перед обнулением
            long nextLinkAddress = node.getLink()[0];

            //Обнуляем информацию о ноде
            node.setSize(0);
            node.getLink()[0] = 0;
            node.setType('-');
            node.setName("");
            node.writeNode(file);

            //Затираем в родителе ссылку
            Arrays.sort(parent.getLink());
            int index = Arrays.binarySearch(parent.getLink(), node.getAddress());
            parent.getLink()[index] = 0;
            parent.updateLinks(file);

            //Бежим по кластерами и обнуляем инфу и ссылки
            //Сортируем пустые индексы чтобы в начале появились занятые кластеры
            Arrays.sort(emptyClusters);
            int fullClusterIndex = 0;
            while (nextLinkAddress != -1) {
                Cluster c = Cluster.getClusterAtPoint(file, nextLinkAddress);
                nextLinkAddress = c.getLink();

                c.setSize(0);
                c.setLink(-1);
                c.writeClusterInfo(file);

                //Вычисляем пустой индекс
                emptyClusters[fullClusterIndex] = c.getClusterIndex();
                fullClusterIndex++;
            }

        } else if (node.getType() == 'd') {
            //Если директория, то смотрим чтобы не было линков и только тогда удаляем,
            //после чего добавляем ее в список свободных нод

            //Смотрим есть ли у ноды ссылки(пустая или нет)
            Arrays.sort(node.getLink());
            if (node.getLink()[Settings.MAX_DIR_COUNT - 1] > 0) {
                throw new Exception("Directory not empty");
            }

            node.setName("");
            node.setSize(0);
            node.setType('-');

            node.writeNode(file);

            Arrays.sort(parent.getLink());
            int index = Arrays.binarySearch(parent.getLink(), node.getAddress());
            parent.getLink()[index] = 0;
            parent.updateLinks(file);

            Arrays.sort(emptyNodes);
            //Доавляем адрес ноды в свободный список
            emptyNodes[0] = node.getAddress();

        }
    }

    /**
     * Moves node from parent to new parent
     *
     * @param node      Node to move
     * @param parent    node which contains node
     * @param newParent new parent node
     * @throws Exception any container problem, or logic checks fail
     */
    public void move(Node node, Node parent, Node newParent) throws Exception {
        if (node.getAddress() == 0) {
            throw new Exception("Cannot delete root");
        }
        if (parent.getType() != 'd' || newParent.getType() != 'd') {
            throw new Exception("One of parents is not directory");
        }

        //Ищем свободное место для новой ссылки и проверяем уникальность имен
        int index = -1;
        for (int i = 0; i < newParent.getLink().length; i++) {
            //Ищем пустую ссылку
            if (index == -1 && newParent.getLink()[i] == 0) {
                index = i;
            }

            //Проверяем имя ноды и имя ссылки на уникальность
            if (Arrays.equals(node.getName(), Node.getNodeAtPoint(file, newParent.getLink()[i]).getName())) {
                throw new Exception("Duplicate node name");
            }
        }

        if (index == -1) {
            throw new Exception("No empty links");
        }

        //Вытаскиваем адрес из старого родителя
        Arrays.sort(parent.getLink());
        int index2 = Arrays.binarySearch(parent.getLink(), node.getAddress());
        if (index2 < 0) {
            throw new Exception("Parent doesn't contain node");
        }
        //Пишем в нового родителя ссылку на ноду
        newParent.getLink()[index] = node.getAddress();
        //А в старом затираем ссылку
        parent.getLink()[index2] = 0;
        parent.updateLinks(file);
        newParent.updateLinks(file);
    }

    public Node getNodeByPath(long link) throws IOException {
        return Node.getNodeAtPoint(file, link);
    }

    public Cluster getClusterByPath(long link) throws IOException {
        return Cluster.getClusterAtPoint(file, link);
    }

}


