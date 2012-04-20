package ru.mltny.vfs;

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


    //состояние кластеров
    //Получаем на 10гб(20971520 clusters) 85мб занимает только хранение состояний кластеров
    private int[] emptyClusters = new int[Settings.CLUSTER_COUNT];

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
        for (int i = 0; i < emptyClusters.length; i++) {
            file.seek(Settings.CLUSTER_FIRST_ADDRESS + i * Cluster.getObjectSize());
            //Если кластар занят то значение его = 0
            if (file.readInt() > 0) {
                emptyClusters[i] = 0;
            } else {
                //сли кластре свободен то хранием его индекc
                emptyClusters[i] = i;
            }
        }
        System.out.println("Update container: " + (System.currentTimeMillis() - time1));
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
            if (i % (Settings.NODE_COUNT / 10) == 0) {
                System.out.print(i * 100 / Settings.NODE_COUNT + "%..");
            }
            Node node2 = new Node();
            node2.setSize(0);
            node2.setType('-');
            node2.setAddress(i * Node.getObjectSize());
            node2.writeNode(file);
        }

        System.out.println("Create nodes: " + (System.currentTimeMillis() - time1));

        time1 = System.currentTimeMillis();
        //заполняем инфу о кластерах
        for (int i = 0; i < Settings.CLUSTER_COUNT; i++) {
            if (i % (Settings.CLUSTER_COUNT / 10) == 0) {
                System.out.print(i * 100 / Settings.CLUSTER_COUNT + "%..");
            }
            Cluster cluster = new Cluster(-1, 0, Settings.CLUSTER_FIRST_ADDRESS + i * Cluster.getObjectSize());
            cluster.writeClusterInfo(file);
        }
        System.out.println("Create clusters: " + (System.currentTimeMillis() - time1));

        updateContainerInfo();
    }

    /**
     * Create node of special type with link to parent node
     *
     * @param type   'f' - file , 'd'-folder, '-' - free node
     * @param name   name of node(will autocut to 16chars)
     * @param parent node which will store link to created node
     * @throws IOException
     */
    public void create(char type, String name, Node parent) throws Exception {
        //Ищем пустую ноду
        //todo maloletniy проседает при создании, посмотреть, скорее сего поиск свободной ноды
        long freeNodePointer = -1;

        for (int i = 1; i < Settings.NODE_COUNT; i++) {
            Node n = Node.getNodeAtPoint(file, Settings.NODE_SIZE_BYTES * i);

            //проверка на уникальность имени создаваемого файла
            if (Arrays.equals(n.getName(), name.toCharArray())) {
                throw new Exception("Non unique name");
            }

            // нода пустая и мы еще не нашли пустую ноду;
            if (freeNodePointer == -1 && n.getType() == '-') {
                freeNodePointer = n.getAddress();
            }
        }

        //Если пустой ноды не нашлось то извиняйте
        if (freeNodePointer == -1) {
            throw new Exception("Node limit error");
        }

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
    }

    public void write(Node node, byte[] bytes) throws Exception {
        if (node.getType() != 'f') {
            throw new Exception("cannot write - not a file: \n" + node);
        }

//        int oldClusterCount = (int) Math.ceil((double) node.getSize() / Settings.CLUSTER_SIZE_BYTES);
        int newClusterCount = (int) Math.ceil((double) bytes.length / Settings.CLUSTER_SIZE_BYTES);
        long firstClusterLink;
        //Смотрим есть ли уже ссылки на кластера
        if (node.getLink()[0] != 0) {
            firstClusterLink = node.getLink()[0];

            //todo maloletniy сделать перезапись уже существующих данных
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
                        while (link != -1) {
                            //Ищем слд связаный кластер
                            Cluster c = Cluster.getClusterAtPoint(file, link);
                            link = c.getLink();
                            int index = (int) ((c.getAddress() - Settings.CLUSTER_FIRST_ADDRESS) / Cluster.getObjectSize());

                            //Обновляем список свободных кластеров
                            //todo maloletny подумать нужно ли каждый раз делать сортировку
                            Arrays.sort(emptyClusters);
                            emptyClusters[0] = index;

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

    public void append(long node, byte[] bytes) throws Exception {
        throw new Exception("not implemented yet");
        //Найти конец существующей ноды и дописать в нее даные
    }

    public void rename(String path, String name) throws Exception {
        throw new Exception("not implemented yet");
        //тупо переименовать ноду
    }

    public void delete(String path) throws Exception {
        throw new Exception("not implemented yet");
        //а вот тут ндо подумать как отмечать удаленные данные
    }

    public void move(String path1, String path2) throws Exception {
        throw new Exception("not implemented yet");
        //тупо переместить адрес ноды
    }

    public Node getNodeByPath(long link) throws IOException {
        return Node.getNodeAtPoint(file, link);
    }

    public Cluster getClusterByPath(long link) throws IOException {
        return Cluster.getClusterAtPoint(file, link);
    }

}


