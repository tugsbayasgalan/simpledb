package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;



/**
 * 
 * @author tugsuu
 * This class handles the locks of transactions
 */
public class LockManager {
	
	private final ConcurrentMap<PageId, Object> lockHolders;
	
	private final HashMap<PageId, HashSet<TransactionId>> sharedLocks;
	private final HashMap<PageId, TransactionId> exclusiveLocks;
	
	
	
	public LockManager() {
		this.lockHolders = new ConcurrentHashMap<>();
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		this.sharedLocks =  new HashMap<PageId, HashSet<TransactionId>>();
	}
	
	public void releaseLock(TransactionId tid, PageId pid) {
		throw new RuntimeException("not implemented yet");
		
	}
	
	public void releasePage(TransactionId tid, PageId pid) {
		throw new RuntimeException("not implemented yet");
	}

	public void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
		
		// read permission
		if (Permissions.READ_ONLY.equals(perm)) {
			HashSet<TransactionId> currentSet = sharedLocks.getOrDefault(pid, new HashSet<TransactionId>());
			currentSet.add(tid);
			sharedLocks.put(pid, currentSet);
		}
		
		
		// write permission
		else if (Permissions.READ_WRITE.equals(perm)) {
			exclusiveLocks.put(pid, tid);
		}
		
		
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		// TODO Auto-generated method stub
		if (lockHolders.containsKey(pid)) {
			return true;
		}
		return false;
	}

}
