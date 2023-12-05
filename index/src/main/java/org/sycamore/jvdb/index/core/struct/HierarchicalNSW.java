package org.sycamore.jvdb.index.core.struct;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * THIS IS A CLASS
 *
 * @PROJECT_NAME: vector_search_demo
 * @CLASS_NAME: HierarchicalNSW
 * @DESCRIPTION:
 * @CREATER: 桑运昌
 * @DATE: 2023/11/29 0:44
 */
@FunctionalInterface
interface DistFunc<T, R> {
    R apply(T a, T b);
}

@Data
class Node<dataType> {
    private int id;
    private List<Integer> neighbors;
    private dataType data;
    private Integer nodeHighestLayer;
    private double[] vector;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Node myObject = (Node) obj;
        return id == myObject.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

//    public List<Node> getNeighborsFromLayer(Integer layer) {
//       return neighbors.stream().filter(n -> n.nodeHighestLayer >= layer).collect(Collectors.toList());
//    }
}
@Data
@AllArgsConstructor
class NodeWrapper<dataType> implements Comparable<NodeWrapper<dataType>>{
    private Node<dataType> node;
    private double distance;
    public static <dataType>NodeWrapper of(Node<dataType> node, double distance){
        return  new NodeWrapper<>(node,distance);
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NodeWrapper myObject = (NodeWrapper) obj;
        return node.getId() == myObject.node.getId() &&
                distance == myObject.distance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(node.getId(), distance);
    }

    @Override
    public int compareTo(NodeWrapper<dataType> o) {
        return Double.compare(distance, o.distance);
    }
}
public class HierarchicalNSW<dataType> {
    //    public static final int MAX_LABEL_OPERATION_LOCKS = 65536;
//    public static final byte DELETE_MARK = 0x01;
//    private long maxElements;
//    private AtomicLong currentElementCount = new AtomicLong(0); // 当前元素数量
//    private long sizeDataPerElement;
//    private long sizeLinksPerElement;
//    private AtomicLong numDeleted = new AtomicLong(0); // 已删除元素数量
//    private long M;
//    private long maxM;
//    private long maxM0;
//    private long efConstruction;
//    private long ef;
//
//    // 锁机制
//    private List<Lock> labelOpLocks;
//    private Lock globalLock = new ReentrantLock();
//    private List<Lock> linkListLocks;
//
//    private int entryPointNode;
//
//    // 内存管理
//    private long sizeLinksLevel0;
//    private long offsetData, offsetLevel0, labelOffset;
//
//    private byte[] dataLevel0Memory;
//    private byte[][] linkLists;
//    private List<Integer> elementLevels; // 每个元素的级别
//
//    private long dataSize;
//
//
//    //====================================
//    private DistFunc<dataType, Integer> distFunc;
//    private Object distFuncParam;
//    //====================================
//
//
//    // 用于随机数生成
//    private Random levelGenerator;
//    private Random updateProbabilityGenerator;
//
//    // 性能度量
//    private AtomicLong metricDistanceComputations = new AtomicLong(0);
//    private AtomicLong metricHops = new AtomicLong(0);
//
//    private boolean allowReplaceDeleted; // 标记是否允许替换删除的元素
//
//    private Lock deletedElementsLock = new ReentrantLock(); // 已删除元素的锁
//    private Set<Integer> deletedElements; // 存储已删除元素的内部 ID

    private AtomicLong elementCount = new AtomicLong(0);

    private HashMap<Integer,Node<dataType>> nodes = new HashMap<>();

    private Node<dataType> enterNode;
    public synchronized void  setNodes(Node ... addNodes ) {
        for (Node node : addNodes) {
            long andIncrement = elementCount.getAndIncrement();
            nodes.put((int)andIncrement,node);
        }
    }


    //最大层数
    private int maxLevel;
    //加入节点是连接最近邻的最大数
    private int M;
    //每个节点的最大邻居数
    private int Mmax;


    public HierarchicalNSW() {

    }

    public List<Node> getNeighborsFromLayer(Integer layer, Integer id) {
        List<Node<dataType>> neighbors = this.nodes.get(id).getNeighbors().stream().map(nid -> {
            return this.nodes.get((int) nid);
        }).collect(Collectors.toList());
        return neighbors.stream().filter(n -> n.getNodeHighestLayer() >= layer).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        Node<Integer> node1 = new Node<Integer>();
        node1.setId(0);
        node1.setNodeHighestLayer(0);
        node1.setData(1);
        node1.setVector(new double[]{1, 1});
        Node<Integer> node2 = new Node<Integer>();
        node2.setId(1);
        node2.setNodeHighestLayer(0);
        node2.setData(2);
        node2.setVector(new double[]{2, 2});
        Node<Integer> node3 = new Node<Integer>();
        node3.setId(2);
        node3.setNodeHighestLayer(0);
        node3.setData(3);
        node3.setVector(new double[]{1, 3});
        Node<Integer> node4 = new Node<Integer>();
        node4.setId(3);
        node4.setNodeHighestLayer(0);
        node4.setData(4);
        node4.setVector(new double[]{2, 4});


        Node<Integer> node5 = new Node<Integer>();
        node5.setId(4);
        node5.setNodeHighestLayer(0);
        node5.setData(5);
        node5.setVector(new double[]{10, 10});

        List<Integer> list1 = Arrays.asList(node2.getId(), node3.getId(), node4.getId());
        node1.setNeighbors(list1);

        List<Integer> list2 = Arrays.asList(node1.getId(), node4.getId());
        node2.setNeighbors(list2);

        List<Integer> list3 = Arrays.asList(node1.getId(), node4.getId());
        node3.setNeighbors(list3);

        List<Integer> list4 = Arrays.asList(node1.getId(), node2.getId(), node3.getId());
        node4.setNeighbors(list4);

        HierarchicalNSW<Integer> integerHierarchicalNSW = new HierarchicalNSW<>();
        integerHierarchicalNSW.setNodes(node1, node2, node3, node4);

        List<NodeWrapper<Integer>> nodeWrappers = integerHierarchicalNSW.searchLayer(node5, node4, 2, 1);
        for (NodeWrapper<Integer> nodeWrapper : nodeWrappers) {
            System.out.println(nodeWrapper.getNode().getId());
        }

    }

    private Integer getNewNodeHighestLayer(int maxLevel) {
        double ml = 1 / Math.log(maxLevel);
        Random rand = new Random();
        // 生成一个 0 到 1 之间的随机数
        double unif = rand.nextDouble();
        // 计算新元素的层级
        int level = (int) Math.floor(-Math.log(unif) * ml);
        // 确保层级不超过最大层数
        level = Math.min(level, maxLevel);
        return level;
    }

    private static double getDistance(Node node, Node en) {
        double[] u = node.getVector();
        double[] v = en.getVector();
        double sum = 0.0;
        for (int i = 0; i < u.length; i++) {
            sum += Math.pow(u[i] - v[i], 2);
        }
        return Math.sqrt(sum);
    }
    // 在某一层中搜索ef个最近邻
    private List<NodeWrapper<dataType>> searchLayer(Node<dataType> node, Node<dataType> entryPoint, Integer ef, Integer layer){
        if (node.getNodeHighestLayer() < layer) return new ArrayList<NodeWrapper<dataType>>();
        if (entryPoint.getNodeHighestLayer() < layer) return new ArrayList<NodeWrapper<dataType>>();
        Set<Integer> visited = new HashSet<>();
        ArrayList<Integer> targetNodes = new ArrayList<>();
        ArrayList<NodeWrapper<dataType>> res = new ArrayList<>();
        targetNodes.add(entryPoint.getId());
        while (targetNodes.size() > 0){
            ArrayList<Integer> candidates = new ArrayList<>();
            for (int i = 0; i < targetNodes.size(); i++) {
                if (visited.contains(targetNodes.get(i))) continue;
                visited.add(targetNodes.get(i));
                Node<dataType> tNode = nodes.get(targetNodes.get(i));
                if (tNode.getNodeHighestLayer() < layer) continue;
                res.add(NodeWrapper.of(nodes.get(targetNodes.get(i)), getDistance(node, nodes.get(targetNodes.get(i)))));
                candidates.addAll(nodes.get(targetNodes.get(i)).getNeighbors());
            }
            targetNodes = new ArrayList<>();
            targetNodes.addAll(candidates);
        }
       return res.stream().sorted().collect(Collectors.toList()).subList(0, ef) ;
    }

    private Node[] searchNeighbors(Node node, List<Node> W, Integer M, Integer layer) {
        return null;
    }


    public void addPoint(Node node, Integer ef) {
        if (elementCount.get() == 0){
            //首次加入node
            synchronized (this) {
                if (elementCount.get() == 0) {
                    node.setId(0);
                    node.setNodeHighestLayer(this.maxLevel);
                    this.enterNode = node;
                    this.elementCount.incrementAndGet();
                    this.nodes.put(0, node);
                    return;
                }
            }
        }

        //获取当前节点最大层数
        Integer l = getNewNodeHighestLayer(maxLevel);


        Node en = this.enterNode;
        Integer L = enterNode.getNodeHighestLayer();


        List<Node> W = new ArrayList<>();
        for (int layer = L; layer > l; layer--) {
            List<Node> nodes = searchLayer(node, en, 1, layer);
            W = nodes;
            en = nodes.get(0);
        }
        for (int layer = L < l ? L : l;
             layer >= 0;
             layer--) {
            List<Node> nodes = searchLayer(node, en, ef, layer);
            W = nodes;
            en = nodes.get(0);
            Node[] neighbors = searchNeighbors(node, W, ef, layer);
            for (Node neighbor : neighbors) {
                neighbor.getNeighbors().add(node);
            }
            for (Node neighbor : neighbors) {
                if (neighbor.getNeighbors().size() > Mmax) {
                    neighbor.setNeighbors(
                            Arrays.asList(
                                    searchNeighbors(node, neighbor.getNeighbors(), Mmax, layer)
                            )
                    );
                }
            }

        }

        if (l > L) this.enterNode = node;
    }


}
