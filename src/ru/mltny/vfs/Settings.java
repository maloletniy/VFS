package ru.mltny.vfs;

import ru.mltny.vfs.units.Node;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/18/12
 * Time: 1:59 PM
 * Default object container params
 */
public final class Settings {

    public static final short MAX_DIR_COUNT = 16;

    public static final short MAX_NAME_LENGTH = 16;

    public static final int CLUSTER_SIZE_BYTES = 1024;

    public static final int CLUSTER_COUNT = 100000;

    public static final int NODE_COUNT = CLUSTER_COUNT / 4;

    public static final long CLUSTER_FIRST_ADDRESS = NODE_COUNT * Node.getObjectSize();

}
