package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WaitsForGraph {
    
    Map<TransactionId, Node> nodes = new HashMap<>();
    
    private class Node {
        TransactionId tid;
        Map<TransactionId, Set<PageId>> nexts;
        Map<PageId, Set<TransactionId>> prevs;
        
        Node(TransactionId tid) {
            this.tid = tid;
        }
    }
    
    public synchronized boolean hasCycle(TransactionId startTid) {
        return false;
    }
    
    public synchronized void addWaitsFor(TransactionId tid1, PageId pid, TransactionId tid2) {
        
    }
    
    public synchronized void deleteWaitsFor(TransactionId tid1, PageId pid, TransactionId tid2) {
        
    }
}
