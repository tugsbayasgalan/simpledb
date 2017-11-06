package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
	
	private final HashMap<TransactionId, HashSet<PageId>> sharedPages;
	private final HashMap<TransactionId, HashSet<PageId>> exclusivePages;
	
	
	
	
	
	
	
	public LockManager() {
		this.lockHolders = new ConcurrentHashMap<>();
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		this.sharedLocks =  new HashMap<PageId, HashSet<TransactionId>>();
		this.sharedPages = new HashMap<TransactionId, HashSet<PageId>>();
		this.exclusivePages = new HashMap<TransactionId, HashSet<PageId>>();
		
	}
	
	public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		long start = System.currentTimeMillis();
		if (perm.equals(Permissions.READ_ONLY)) {
			
			while(!acquireReadLock(tid, pid)) {
				try {
					Thread.sleep(20);
				} catch (Exception e){
					e.printStackTrace();
					System.out.println("Error occured while waiting");
				}
				
				long current = System.currentTimeMillis();
				if (current - start > 5000) {
					throw new TransactionAbortedException();
				}
				
			}
			
			return true;
			
		}
		
		if (perm.equals(Permissions.READ_WRITE)) {
			
			while(!acquireReadWriteLock(tid, pid)) {
				try {
					Thread.sleep(20);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Error occured while waiting");
				}
				
				long current = System.currentTimeMillis();
				if (current - start > 5000) {
					throw new TransactionAbortedException();
				}
			}
			return true;
			
		}
		
		else {
			throw new IllegalArgumentException("Only read and read write are acceptaple");
		}
		
		
	}
	
	private boolean acquireReadLock(TransactionId tid, PageId pid) {
		
		
		
		
		// if doesn't have the exclusive lock yet
		if (!exclusiveLocks.containsKey(pid)) {
			
			if (sharedLocks.containsKey(pid)) {
				sharedLocks.get(pid).add(tid);
				
			}
			
			if (!sharedLocks.containsKey(pid)) {
				HashSet<TransactionId> tids = new HashSet<>();
				tids.add(tid);
				sharedLocks.put(pid, tids);
				return true;
			}
			
			if (sharedPages.containsKey(tid)) {
				sharedPages.get(tid).add(pid);
			}
			
			if (!sharedPages.containsKey(tid)) {
				HashSet<PageId> pids = new HashSet<>();
				pids.add(pid);
				sharedPages.put(tid, pids);
				
			}
			
			return true;
			
		} 
		
		// if it has write lock
		else {
			TransactionId exclusiveTransaction = exclusiveLocks.get(pid);
			
			if (exclusiveTransaction != null && exclusiveTransaction.equals(tid)) {
				return true;
			}
			
			else {
				return false;
			}
		}
		
		
		
		
		
	}
	
	private boolean acquireReadWriteLock(TransactionId tid, PageId pid) {
		
		TransactionId notNullTd;
		
		
		
		if (tid == null) {
			notNullTd = new TransactionId();
		}
		
		else {
			notNullTd = tid;
		}
		
		
		
		if (exclusiveLocks.containsKey(pid)) {
			if (exclusiveLocks.get(pid).equals(notNullTd)) {
				return true;
			}
			return false;
		}
		
		else {
			if (sharedLocks.containsKey(pid)) {
				
				
				HashSet<TransactionId> tids = sharedLocks.get(pid);
				if (tids.contains(tid)) {
					if (tids.size() == 1) {
						exclusiveLocks.put(pid, notNullTd);
						if (exclusivePages.containsKey(tid)) {
							exclusivePages.get(tid).add(pid);
						}
						
						else {
							HashSet<PageId> pids = new HashSet<>();
							pids.add(pid);
							exclusivePages.put(notNullTd, pids);
						}
						
						sharedLocks.get(pid).remove(notNullTd);
						
						if (sharedLocks.get(pid).size() == 0) {
							sharedLocks.remove(pid);
						}
						
						//System.out.println(sharedPages);
						if (sharedPages.containsKey(notNullTd)) {
							sharedPages.get(notNullTd).remove(pid);
							if (sharedPages.get(notNullTd).size() == 0) {
								sharedPages.remove(notNullTd);
							}
						}
						
						
						
						return true;
					}
					
					else {
						
						return false;
					}
					
				} else {
					return false;
				}
				
			}
			
			else {
				exclusiveLocks.put(pid, notNullTd);
				
				if (exclusivePages.containsKey(notNullTd)) {
					exclusivePages.get(notNullTd).add(pid);
				}
				
				else {
					HashSet<PageId> pids = new HashSet<PageId>();
					pids.add(pid);
					exclusivePages.put(notNullTd, pids);
				}
				
				return true;
			}
		}
		
		
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
	
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		
		if (tid != null && pid != null) {
			
			if (sharedPages.containsKey(tid)) {
				sharedPages.get(tid).remove(pid);
			}
			
			if (exclusivePages.containsKey(tid)) {
				exclusivePages.get(tid).remove(pid);
			}
			
			if (sharedLocks.containsKey(pid)) {
				sharedLocks.get(pid).remove(tid);
			}
			
			if (exclusiveLocks.containsKey(pid)) {
				exclusiveLocks.remove(pid);
			}
			
		}
		
		else if (tid == null) {
			if (sharedLocks.containsKey(pid)) {
				
				HashSet<TransactionId> tids = sharedLocks.get(pid);
				
				for (TransactionId transaction: tids) {
					if (sharedPages.containsKey(transaction)) {
						sharedPages.get(transaction).remove(pid);
						if (sharedPages.get(transaction).size() == 0) {
							sharedPages.remove(transaction);
						}
					}
				}
				
				sharedLocks.remove(pid);
			}
			
			if (exclusiveLocks.containsKey(pid)) {
				TransactionId transaction = exclusiveLocks.get(pid);
				
				if (exclusivePages.containsKey(transaction)) {
					exclusivePages.get(transaction).remove(pid);
					if (exclusivePages.get(transaction).size() == 0) {
						exclusivePages.remove(transaction);
					}
				}
				
				exclusiveLocks.remove(pid);
				
			}
		}
		
		else if (pid == null) {
			
			if (sharedPages.containsKey(tid)) {
				HashSet<PageId> pids = sharedPages.get(tid);
				
				for (PageId pageID: pids) {
					if (sharedLocks.containsKey(pageID)) {
						sharedLocks.get(pageID).remove(tid);
						if (sharedLocks.get(pageID).size() == 0) {
							sharedLocks.remove(pageID);
						}
					}
				}
				
				sharedPages.remove(tid);
			}
			
			if (exclusivePages.containsKey(tid)) {
				HashSet<PageId> pids = exclusivePages.get(tid);
				
				for (PageId pageID: pids) {
					if (exclusiveLocks.containsKey(pageID)) {
						exclusiveLocks.remove(pageID);
					}
				}
				
				exclusivePages.remove(tid);
			}
		}
		
		
		
		
		
	}

	public synchronized Set<PageId> getDirtiedPages(TransactionId tid) {
		
		Set<PageId> result = new HashSet<>();
		
		if (exclusivePages.containsKey(tid)) {
			result.addAll(exclusivePages.get(tid));
		}
		
		return result;
	}

	

}
