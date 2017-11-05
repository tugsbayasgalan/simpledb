package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
	
	private final HashMap<TransactionId, List<TransactionId>> dependency;
	
	
	
	
	
	public LockManager() {
		this.lockHolders = new ConcurrentHashMap<>();
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		this.sharedLocks =  new HashMap<PageId, HashSet<TransactionId>>();
		this.dependency = new HashMap<TransactionId, List<TransactionId>>();
	}
	
	public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
		
		if (perm.equals(Permissions.READ_ONLY)) {
			
			if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
				return true;
			} 
			
			if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
				return true;
			}
			
			while(!acquireReadLock(tid, pid)) {
				// wait until gets shared lock
			}
			
			
			return true;
			
		}
		
		if (perm.equals(Permissions.READ_WRITE)) {
			
			if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
				return true;
			}
			
			while (!acquireReadWriteLock(tid, pid)) {
				// wait until gets exclusive lock
			}
			
			// TODO this is sketchy, might check later
			
			return true;
			
		}
		
		else {
			throw new IllegalArgumentException("Only read and read write are acceptaple");
		}
		
		
	}
	
	private boolean acquireReadLock(TransactionId tid, PageId pid) {
		lockHolders.putIfAbsent(pid, new Object());
		
		Object lock = lockHolders.get(pid);
		
		while(true) {
			synchronized(lock) {
				if (exclusiveLocks.get(pid) == null || exclusiveLocks.get(pid).equals(tid)) {
					
					sharedLocks.putIfAbsent(pid, new HashSet<TransactionId>());
					sharedLocks.get(pid).add(tid);
					
					return true;
				}
				
				
				
			}
		}
	}
	
	private boolean acquireReadWriteLock(TransactionId tid, PageId pid) {
		lockHolders.putIfAbsent(pid, new Object());
		
		Object lock = lockHolders.get(pid);
		
		while(true) {
			synchronized(lock) {
				List<TransactionId> allLockHolders = getLockHolders(pid);
				
				if (isIndependent(tid, allLockHolders)) {
					
					exclusiveLocks.put(pid, tid);
					return true;
				}
			}
		}
	}
	
	private List<TransactionId> getLockHolders(PageId pid){
		
		List<TransactionId> result = new ArrayList<>();
		
		if (sharedLocks.containsKey(pid)) {
			result.addAll(sharedLocks.get(pid));
			return result;
		}
		
		if (exclusiveLocks.containsKey(pid)) {
			result.add(exclusiveLocks.get(pid));
			return result;
		}
		
		return result;
		
	}
	
	private boolean isIndependent(TransactionId tid, List<TransactionId> lockHolders) {
		
		if (lockHolders.isEmpty()) {
			return true;
		}
		
		if (lockHolders.size() == 1) {
			if (lockHolders.get(0).equals(tid)) {
				return true;
				
			}
		}
		
		return false;
	}
	
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
		
		if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
			return true;
		}
		
		if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
			return true;
		}
		
		return false;
	}
	
	public void releasePage(TransactionId tid, PageId pid) {
		
		Object lock = lockHolders.get(pid);
		
		synchronized(lock) {
			exclusiveLocks.remove(pid);
			if (sharedLocks.containsKey(pid)) {
				if (sharedLocks.get(pid).contains(tid)) {
					sharedLocks.get(pid).remove(tid);
				}
			}
		}
		
	}


}
