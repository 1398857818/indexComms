

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.list_privileges;





 class PTrieTree implements java.io.Serializable {

    public PTrieTree() {

    }

    private PTrieTree.TrieNode root = new PTrieTree.TrieNode();

    public TrieNode getRoot() {
        return this.root;
    }

//    public long counter = 0;//Record the size of the PTrieTree

    public class TrieNode implements java.io.Serializable {
        //        private List<String> indexList = null;
        private List<String> indexList = new ArrayList<String>();

        private long index = -1L;
        boolean isLeaf = false;
        private Map<Character, PTrieTree.TrieNode> son = new TreeMap();

        TrieNode() {
        }

        public List<String> getIndexlist() {
            return this.indexList;
        }
    }

    public void insert(String key, String val) {



        //判断字符串长度，返回不同的结果

        if(!StringUtils.isBlank(key)) {
//            counter++;

            PTrieTree.TrieNode node = this.root;
            char[] letters = key.toCharArray();


            for(int i = 0; i < letters.length; ++i) {
                char c = letters[i];
                if(node.son.containsKey(c) && i <= letters.length - 1) {

                    /**
                     * 重复插入，相同的字符串
                     */
                    node = node.son.get(c);
                    if (i == letters.length - 1) {
                        node.indexList.add(val);
                    //    System.out.println(val);

                    }
                } else {
                    PTrieTree.TrieNode trieNode = new PTrieTree.TrieNode();


                    if(i == letters.length - 1) {

                        //trieNode.indexList  = new ArrayList<String>();
                        trieNode.indexList.add(val);
                        trieNode.isLeaf = true;
                     //   System.out.println(val);

                    }

                    node.son.put(c, trieNode);
                    node = trieNode;

                }

            }

        }
    }

    public List<String> getIndex(String key) {

        if(StringUtils.isBlank(key)) {
            List<String> err1 = new ArrayList<String>();
            err1.add("err1");
            return err1;

        } else {
            PTrieTree.TrieNode node = this.root;
            char[] letters = key.toCharArray();

            for(int i = 0; i < letters.length; ++i) {
                char c = letters[i];

                if(!node.son.containsKey(c)) {
                    List<String> err2 = new ArrayList<String>();
                    err2.add("err2");
                    return err2;  //找不到key
                }

                if(i == letters.length - 1) {

                    /**
                     * 根据商品编号 查找该Gid下的所有的Uid结点的评论
                     */
                    if (i == 7 && letters.length == 8) {
                        preTraverse(node.son.get(c));
                    }
                    //System.out.println(node.son.get(c).indexList);
                    return node.son.get(c).indexList;

                }

                node = node.son.get(c);
            }

            //这里是没有返回值异常吗
            List<String> err3 = new ArrayList<String>();
            err3.add("err3");
            return err3;
        }
    }



    // 前序遍历字典树.
    public List<String> preTraverse(TrieNode node) {
        if (node != null) {
            //System.out.print(node.val + "-");
            for (TrieNode child : node.son.values()) {

                preTraverse(child);
                node.indexList.addAll(child.indexList);

                /**
                 * node.indexList.addAll(child.indexList);
                 * preTraverse(child);
                 * 因为返回的是 node.indexList,尽管第二步使child改变了，但是第一步的node是由为改变的child决定
                 */
            }
            return node.indexList;
        }
        return node.indexList;
    }
    /**
     // 前序遍历字典树.
     public void preTraverse(TrieNode node) {
     if (node != null) {
     //System.out.print(node.val + "-");
     for (TrieNode child : node.son.values()) {
     if (child.isLeaf == false) {
     preTraverse(child);
     }

     node.indexList.addAll(child.indexList); //这个代码使孩子的全部结点的indexList全部放在父节点的indexList

     }
     }
     }

     /*    public boolean upgrade(String key, long index) {
     if(StringUtils.isBlank(key)) {
     return false;
     } else {
     PTrieTree.TrieNode node = this.root;
     char[] letters = key.toCharArray();

     for(int i = 0; i < letters.length; ++i) {
     char c = letters[i];
     if(i == letters.length - 1) {
     ((PTrieTree.TrieNode)node.son.get(Character.valueOf(c))).index = index;
     return true;
     }

     if(!node.son.containsKey(Character.valueOf(c))) {
     return false;
     }

     node = (PTrieTree.TrieNode)node.son.get(Character.valueOf(c));
     }

     return false;
     }
     }*/


}
