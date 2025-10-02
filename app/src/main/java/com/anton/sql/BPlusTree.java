package com.anton.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.anton.storage.Slot;

public class BPlusTree<K extends Comparable<K>> {
	// max number of keys per node
	private final int ORDER;
	// min number keys per node
	private final int MIN_KEYS;
	// root of the B+ Tree
	private BPlusTreeNode<K> root;
	// Read Write lock for thread safety
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/* ========================== NODE HIERARCHY ====================== */

	// Abstract base class for B+ Tree Nodes
	public static abstract class BPlusTreeNode<K extends Comparable<K>> {
		// pointers to traverse on the tree
		protected volatile BPlusTreeNode<K> parent;
		// max keys per node
		protected final int maxSize;

		public BPlusTreeNode(int maxSize) {
			this.parent = null;
			this.maxSize = maxSize;
		}

		// Abstract methods to be implemented by subclasses
		public abstract int size();
		public abstract boolean isFull();
		public abstract boolean isUnderflow(int minSize);
		public abstract K getMinKey();
		public abstract K getMaxKey();
		public abstract boolean isLeaf();

		// Common functionality
		public BPlusTreeNode<K> getParent() {
			return this.parent;
		}

		public void setParent(BPlusTreeNode<K> parent) {
			this.parent = parent;
		}
	}

	// Represents a Routing block in internal nodes
	// Contains key and pointer to the child node
	public static final class Router<K extends Comparable<K>> implements Comparable<Router<K>> {
		public final K key;
		public final BPlusTreeNode<K> child;

		public Router(K key, BPlusTreeNode<K> child) {
			if (key == null || child == null) {
				throw new IllegalArgumentException("Key and child can not be null.");
			}

			this.key = key;
			this.child = child;
		}

		@Override
		public int compareTo(Router<K> router) {
			return this.key.compareTo(router.key);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof Router)) return false;

			Router<?> router = (Router<?>) obj;
			return this.key.equals(router.key);
		}

		@Override
		public int hashCode() {
			return this.key.hashCode();
		}

		@Override
		public String toString() {
			return String.format("Routing{key=%s}", key);
		}
	}

	// Represents a key-value pair block in leaf nodes
	// Immutable for thread safety
	public static final class Entry<K extends Comparable<K>> implements Comparable<Entry<K>> {
		public final K key; // -> Index
		public final Slot value; // Slot - of the data entry in the database

		public Entry(K key, Slot slot) {
			if (key == null || slot == null) {
				throw new IllegalArgumentException("Key and slot can not be null.");
			}

			this.key = key;
			this.value = slot;
		}

		@Override
		public int compareTo(Entry<K> entry) {
			return this.key.compareTo(entry.key);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof Entry)) return false;

			Entry<?> entry = (Entry<?>) obj;
			return this.key.equals(entry.key);
		}

		@Override
		public int hashCode() {
			return this.key.hashCode();
		}

		@Override
		public String toString() {
			return String.format("Entry{key=%s, value=%s}", this.key, this.value);
		}
	}

	// Internal Node
	// Stores routing information and child pointers
	public static final class InternalNode<K extends Comparable<K>> extends BPlusTreeNode<K> {
		private List<Router<K>> routers;
		private volatile BPlusTreeNode<K> firstChild; // Leftmost child (for keys < first routing key)

		public InternalNode(int maxSize) {
			super(maxSize);
			this.routers = new ArrayList<>(maxSize + 1); // +1 for splitting the node
			this.firstChild = null;
		}

		@Override
		public int size() {
			return this.routers.size();
		}

		@Override
		public boolean isFull() {
			return this.routers.size() >= this.maxSize;
		}

		@Override
		public boolean isUnderflow(int minSize) {
			return this.routers.size() < minSize;
		}

		@Override
		public K getMinKey() {
			return this.routers.isEmpty() ? null : this.routers.get(0).key;
		}

		@Override
		public K getMaxKey() {
			return this.routers.isEmpty() ? null : this.routers.get(this.routers.size() - 1).key;
		}

		@Override
		public boolean isLeaf() {
			return false;
		}

		// Internal node specific methods

		public List<Router<K>> getRouters() {
			return Collections.unmodifiableList(this.routers);
		}

		public BPlusTreeNode<K> getFirstChild() {
			return this.firstChild;
		}

		public void setFirstChild(BPlusTreeNode<K> child) {
			this.firstChild = child;
			if (child != null) {
				this.firstChild.setParent(this);
			}
		}

		public void addRouter(Router<K> router) {
			int insertPos = Collections.binarySearch(this.routers, router);
			if (insertPos < 0) {
				insertPos = -(insertPos + 1);
			}
			this.routers.add(insertPos, router);
			router.child.setParent(this);
		}

		public void removeRouter(K key) {
			this.routers.removeIf(r -> r.key.equals(key));
		}

		public void replaceRouter(K oldKey, Router<K> newRouter) {
			for (int i = 0; i < this.routers.size(); i++) {
				if (this.routers.get(i).key.equals(oldKey)) {
					this.routers.set(i, newRouter);
					newRouter.child.setParent(this);
					return;
				}
			}
		}

		// to find the child within an internal node
		public BPlusTreeNode<K> findChild(K key) {
			if (this.routers.isEmpty()) {
				return this.firstChild;
			}

			for (int i = 0; i < this.routers.size(); i++) {
				if (key.compareTo(this.routers.get(i).key) < 0) {
					return i == 0 ? this.firstChild : this.routers.get(i - 1).child;
				}
			}

			// if key >= the last rotuer's key
			return this.routers.get(this.routers.size() - 1).child;
		}
		
		public List<BPlusTreeNode<K>> getAllChildren() {
			List<BPlusTreeNode<K>> children = new ArrayList<>();

			if (this.firstChild != null) {
				children.add(this.firstChild);
			}
			for (Router<K> router : this.routers) {
				children.add(router.child);
			}

			return children;
		}

		public List<Router<K>> split() {
			int splitPoint = (this.routers.size() + 1) / 2;
			List<Router<K>> rightRouters = new ArrayList<>(this.routers.subList(splitPoint, this.routers.size()));
			this.routers.subList(splitPoint, this.routers.size()).clear();
			return rightRouters;
		}

		@Override
		public String toString() {
			return String.format("InternalNode{size=%d, routers=%ds}", this.routers.size(), this.routers);
		}
	}

	public static final class LeafNode<K extends Comparable<K>> extends BPlusTreeNode<K> {
		private final List<Entry<K>> entries;
		private volatile LeafNode<K> next;
		private volatile LeafNode<K> previous;

		public LeafNode(int maxSize) {
			super(maxSize);
			this.entries = new ArrayList<>(maxSize + 1); // +1 for overflow during node splits
			this.next = null;
			this.previous = null;
		}

		@Override
		public int size() {
			return this.entries.size();
		}

		@Override
		public boolean isFull() {
			return this.entries.size() >= maxSize;
		}
		
		@Override
		public boolean isUnderflow(int minSize) {
			return this.entries.size() < minSize;
		}

		@Override
		public K getMinKey() {
			return this.entries.isEmpty() ? null : this.entries.get(0).key;
		}

		@Override
		public K getMaxKey() {
			return this.entries.isEmpty() ? null : this.entries.get(this.entries.size() - 1).key;
		}

		@Override
		public boolean isLeaf() {
			return true;
		}

		// Leaf Node specific methods

		public List<Entry<K>> getEntries() {
			return this.entries;
		}

		public Entry<K> getEntry(int index) {
			return this.entries.get(index);
		}

		public void addEntry(Entry<K> entry) {
			int insertPos = Collections.binarySearch(this.entries, entry);

			if (insertPos >= 0) {
				// key already exists, replace the entry
				this.entries.add(insertPos, entry);
			} else {
				// New entry, add at correct position
				this.entries.add(-(insertPos + 1), entry);
			}
		}

		public boolean removeEntry(K key) {
			Entry<K> entry = new Entry<>(key, null); // temporary entry to find the entry with the same key as given
			return this.entries.removeIf(e -> e.compareTo(entry) == 0);
		}

		public Entry<K> findEntry(K key) {
			Entry<K> entry = new Entry<>(key, null); // temporary entry to find the entry with the same key as given
			int idx = Collections.binarySearch(this.entries, entry);
			return idx < 0 ? null : this.entries.get(idx);
		}

		public List<Entry<K>> split() {
			int splitPoint = this.entries.size() / 2;
			// split the right and left sublists
			List<Entry<K>> rightEntries = new ArrayList<>(this.entries.subList(splitPoint, this.entries.size()));
			this.entries.subList(splitPoint, this.entries.size()).clear();
			return rightEntries;
		}

		// Linked list management
		public LeafNode<K> getNext() {
			return this.next;
		}

		public LeafNode<K> getPrevious() {
			return this.previous;
		}

		public void setNext(LeafNode<K> next) {
			this.next = next;
		}

		public void setPrevious(LeafNode<K> previous) {
			this.previous = previous;
		}

		@Override
		public String toString() {
			return String.format("LeafNode{size=%d, entries=%s}", this.entries.size(), this.entries);
		}
	}

	/* ========================== CONSTRUCTOR ====================== */

	public BPlusTree() {
		this(4); // default order
	}

	public BPlusTree(int order) {
		if (order < 4) {
			throw new IllegalArgumentException("Order must be at least 4.");
		}

		this.ORDER = order;
		this.MIN_KEYS = (order + 1) / 2;
		this.root = new LeafNode<>(order);
	}

	/* ========================== B+ Tree Methods ====================== */

	// insert a key-value pair into the B+ Tree
	public void insert(K key, Slot value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Key and value can not be null.");
		}

		lock.writeLock().lock();
		try {
			LeafNode<K> leafNode = findLeafNode(key);
			Entry<K> entry = new Entry<>(key, value);
			leafNode.addEntry(entry);

			// Split the leaf node, if needed
			if (leafNode.isFull()) {
				splitLeafNode(leafNode);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to insert key: " + key, e);
		} finally {
			lock.writeLock().unlock();
		}
	}

	// Search for a value associated with a given key
	public Slot search(K key) {
		if (key == null) {
			throw new IllegalArgumentException("Key can not be null.");
		}

		lock.readLock().lock();
		try {
			LeafNode<K> leafNode = findLeafNode(key);
			Entry<K> entry = leafNode.findEntry(key);
			return entry != null ? entry.value : null;
		} catch (Exception e) {
			throw new RuntimeException("Failed to search for key: " + key, e);
		} finally {
			lock.readLock().unlock();
		}
	}

	// Delete a key from the B+ Tree
	public boolean delete(K key) {
		if (key == null) {
			throw new IllegalArgumentException("Key can not be null.");
		}

		lock.writeLock().lock();
		try {
			LeafNode<K> leafNode = findLeafNode(key);
			boolean removed = leafNode.removeEntry(key);

			// Merge the leaf node, if needed
			if (
				removed &&
				leafNode.isUnderflow(this.MIN_KEYS) &&
				leafNode != this.root &&
				leafNode.getParent() != null
			) {
				handleUnderflow(leafNode);
			}

			return removed;
		} catch (Exception e) {
			throw new RuntimeException("Failed to delete key: " + key, e);
		} finally {
			lock.writeLock().unlock();
		}
	}

	// rangeQueries: return all entries between the given keys
	public List<Entry<K>> rangeQueries(K startKey, K endKey) {
		if (startKey == null || endKey == null) {
			throw new IllegalArgumentException("Start key and end key can not be null.");
		}

		if (startKey.compareTo(endKey) > 0) {
			throw new IllegalArgumentException("Start key must be less than or equal to end key.");
		}
		
		lock.readLock().lock();
		try {
			List<Entry<K>> entries = new ArrayList<>();
			LeafNode<K> currentNode = findLeafNode(startKey);
			
			while (currentNode != null) {
				// Add entries from current node that fall within the range
				for (Entry<K> e : currentNode.getEntries()) {
					if (e.key.compareTo(startKey) >= 0 && e.key.compareTo(endKey) <= 0) {
						entries.add(e);
					}
				}
				
				// Move to next node if current node's max key is <= endKey
				if (currentNode.getMaxKey() != null && currentNode.getMaxKey().compareTo(endKey) <= 0) {
					currentNode = currentNode.getNext();
				} else {
					break;
				}
			}

			return entries;
		} catch (Exception e) {
			throw new RuntimeException("Failed to range query between keys: " + startKey + " and " + endKey, e);
		} finally {
			lock.readLock().unlock();
		}
	}

	// Get all entries in sorted order
	public List<Entry<K>> getAllEntries() {
		lock.readLock().lock();
		try {
			List<Entry<K>> entries = new ArrayList<>();
			LeafNode<K> currentNode = getFirstLeafNode();

			while (currentNode != null && currentNode.getMaxKey() != null) {
				entries.addAll(currentNode.getEntries());
				currentNode = currentNode.getNext();
			}

			return entries;
		} catch (Exception e) {
			throw new RuntimeException("Failed to get all entries", e);
		} finally {
			lock.readLock().unlock();
		}
	}

	// Check if the tree contains a given key
	public boolean contains(K key) {
		if (key == null) {
			throw new IllegalArgumentException("Key can not be null.");
		}

		return search(key) != null;
	}

	// Get total number of entries in the tree
	public int getTotalEntries() {
		lock.readLock().lock();
		try {
			return getAllEntries().size();
		} catch (Exception e) {
			throw new RuntimeException("Failed to get total number of entries", e);
		} finally {
			lock.readLock().unlock();
		}
	}

	public boolean isEmpty() {
		lock.readLock().lock();
		try {
			return this.root.isLeaf() && this.root.size() == 0;
		} catch (Exception e) {
			throw new RuntimeException("Failed to check if the tree is empty", e);
		} finally {
			lock.readLock().unlock();
		}
	}

	public void clear() {
		lock.writeLock().lock();
		try {
			this.root = new LeafNode<>(this.ORDER);
		} catch (Exception e) {
			throw new RuntimeException("Failed to clear the tree", e);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	/* ========================== PRIVATE HELPER METHODS ====================== */

	// Find the leaf node for a given key
	private LeafNode<K> findLeafNode(K key) {
		BPlusTreeNode<K> currentNode = this.root;
		while (!currentNode.isLeaf()) {
			currentNode = ((InternalNode<K>) currentNode).findChild(key);
		}
		return (LeafNode<K>) currentNode;
	}

	// Get the first (leftmost) leaf node
	private LeafNode<K> getFirstLeafNode() {
		BPlusTreeNode<K> currentNode = this.root;
		while (!currentNode.isLeaf()) {
			currentNode = ((InternalNode<K>) currentNode).getFirstChild();
		}
		return (LeafNode<K>) currentNode;
	}

	// Split a leaf node
	private void splitLeafNode(LeafNode<K> leaf) {
		LeafNode<K> newLeaf = new LeafNode<>(this.ORDER);
		List<Entry<K>> rightEntries = leaf.split(); // this is the new right leaf node that will be added to the right of the current leaf node
		
		for (Entry<K> entry : rightEntries) {
			newLeaf.addEntry(entry);
		}
		
		// Update the linked list -> add new leaf between previous leaf and it's next
		newLeaf.setNext(leaf.getNext());
		newLeaf.setPrevious(leaf);
		if (leaf.getNext() != null) {
			leaf.getNext().setPrevious(newLeaf);
		}
		leaf.setNext(newLeaf);

		// Get the key to promote to the parent
		K promotingKey = newLeaf.getMinKey();

		if (leaf.getParent() != null || this.root.isLeaf()) {
			// only root exists in the tree -> create new root
			InternalNode<K> newRoot = new InternalNode<>(this.ORDER);
			newRoot.setFirstChild(leaf);
			newRoot.addRouter(new Router<K>(promotingKey, newLeaf));
			this.root = newRoot;
		}
		else {
			// Inserting into existing parent
			InternalNode<K> parent = (InternalNode<K>) leaf.getParent();
			newLeaf.setParent(parent);
			parent.addRouter(new Router<K>(promotingKey, newLeaf));

			// Split the parent node, if needed
			if (parent.isFull()) {
				splitInternalNode(parent);
			}
		}
	}

	private void splitInternalNode(InternalNode<K> node) {
		InternalNode<K> newNode = new InternalNode<>(this.ORDER);
		List<Router<K>> rightRouters = node.split();

		// first router of the right routers half becomes the new first child
		if (!rightRouters.isEmpty()) {
			Router<K> firstRouter = rightRouters.remove(0);
			newNode.setFirstChild(firstRouter.child);
		}

		// Add remaining rotuers to the newNode
		for (Router<K> r : rightRouters) {
			newNode.addRouter(r);
		}

		K promotingKey = newNode.getMinKey();

		if (node.getParent() != null || this.root.isLeaf()) {
			// only root exists in the tree -> create new root
			InternalNode<K> newRoot = new InternalNode<>(this.ORDER);
			newRoot.setFirstChild(node);
			newRoot.addRouter(new Router<K>(promotingKey, newNode));
			this.root = newRoot;
		}
		else {
			// Inserting into existing parent
			InternalNode<K> parent = (InternalNode<K>) node.getParent();
			newNode.setParent(parent);
			parent.addRouter(new Router<K>(promotingKey, newNode));

			if (parent.isFull()) {
				splitInternalNode(parent);
			}
		}
	}

	// Handle underflow in leaf after deletion
	private void handleUnderflow(LeafNode<K> leaf) {
		InternalNode<K> parent = (InternalNode<K>) leaf.getParent();
		// leaf is root, no underflow handling
		if (parent.equals(null)) return;

		// Try to borrow from right or left siblings
		LeafNode<K> rightSibling = leaf.getNext();
		LeafNode<K> leftSibling = leaf.getPrevious();

		// try to borrow from siblings (child of the same parent as of leaf) which have enough keys to donate
		if (leftSibling != null && leftSibling.getParent() == parent && leftSibling.size() > this.MIN_KEYS) {
			// borrow from leftSibling
			borrowFromLeftSibling(leaf, leftSibling, parent);
		} else if (rightSibling != null && rightSibling.getParent() == parent && rightSibling.size() > this.MIN_KEYS) {
			// borrow from rightSibling
			borrowFromRightSibling(leaf, rightSibling, parent);
		} else {
			// Merge leaf nodes if enough keys are not present to borrow in either of the siblings
			if (leftSibling != null && leftSibling.getParent() == parent) {
				mergeLeafNodes(leftSibling, leaf, parent);
			} else if (rightSibling != null && rightSibling.getParent() == parent) {
				mergeLeafNodes(leaf, rightSibling, parent);
			}
		}
	}

	// Borrow an entry from left sibling
	private void borrowFromLeftSibling(LeafNode<K> leaf, LeafNode<K> leftSibling, InternalNode<K> parent) {
		// Save the old left sibling max key BEFORE any changes
		K oldLeftMaxKey = leftSibling.getMaxKey();

		List<Entry<K>> leftEntries = leftSibling.getEntries();
		Entry<K> borrowed = leftEntries.get(leftEntries.size() - 1);

		leftSibling.removeEntry(borrowed.key);
		leaf.addEntry(borrowed);

		// Update the parent's router for the left sibling (since its max key changed)
		K newLeftMaxKey = leftSibling.getMaxKey();
		parent.replaceRouter(oldLeftMaxKey, new Router<K>(newLeftMaxKey, leftSibling));
	}

	// Borrow an entry from right sibling
	private void borrowFromRightSibling(LeafNode<K> leaf, LeafNode<K> rightSibling, InternalNode<K> parent) {
		// Save the old right sibling min key BEFORE any changes
		K oldRightMinKey = rightSibling.getMinKey();

		List<Entry<K>> rightEntries = rightSibling.getEntries();
		Entry<K> borrowed = rightEntries.get(0);

		rightSibling.removeEntry(borrowed.key);
		leaf.addEntry(borrowed);

		// Update the parent's router for the right sibling (since its min key changed)
		K newRightMinKey = rightSibling.getMinKey();
		parent.replaceRouter(oldRightMinKey, new Router<K>(newRightMinKey, rightSibling));
	}

	// Merge leaf nodes if enough keys are not present to borrow in either of the siblings
	private void mergeLeafNodes(LeafNode<K> left, LeafNode<K> right, InternalNode<K> parent) {
		for (Entry<K> e : right.getEntries()) {
			left.addEntry(e);
		}

		left.setNext(right.getNext());
		if (right.getNext() != null) {
			right.getNext().setPrevious(left);;
		}

		K rightMinKey = right.getMinKey();
		parent.removeRouter(rightMinKey);

		// check parent for underflow
		if (parent.isUnderflow(this.MIN_KEYS) && parent.equals(this.root)) {
			handleInternalNodeUnderflow(parent);
		} else if (parent.equals(this.root) && parent.size() == 0) {
			// Root has no routers, make left child the new root
			this.root = left;
			left.setParent(null);
		}
	}

	// Borrow a router from left internal sibling
	private void borrowRouterFromLeftSibling(InternalNode<K> node, InternalNode<K> leftSibling, InternalNode<K> parent) {
		// Get the last router from left sibling
		List<Router<K>> leftRouters = leftSibling.getRouters();
		Router<K> borrowedRouter = leftRouters.get(leftRouters.size() - 1);
		
		// Remove from left sibling
		leftSibling.removeRouter(borrowedRouter.key);
		
		// Add to current node as first router
		node.addRouter(borrowedRouter);
		
		// Update parent's router key for the left sibling (since its max key changed)
		K newLeftMaxKey = leftSibling.getMaxKey();
		parent.replaceRouter(borrowedRouter.key, new Router<K>(newLeftMaxKey, leftSibling));
	}

	// Borrow a router from right internal sibling
	private void borrowRouterFromRightSibling(InternalNode<K> node, InternalNode<K> rightSibling, InternalNode<K> parent) {
		// Get the first router from right sibling
		List<Router<K>> rightRouters = rightSibling.getRouters();
		Router<K> borrowedRouter = rightRouters.get(0);
		
		// Remove from right sibling
		rightSibling.removeRouter(borrowedRouter.key);
		
		// Add to current node as last router
		node.addRouter(borrowedRouter);
		
		// Update parent's router key for the right sibling (since its min key changed)
		K newRightMinKey = rightSibling.getMinKey();
		parent.replaceRouter(borrowedRouter.key, new Router<K>(newRightMinKey, rightSibling));
	}

	// Merge current node with left internal sibling
	private void mergeWithLeftSiblingInternal(InternalNode<K> node, InternalNode<K> leftSibling, InternalNode<K> parent) {
		// Move all routers from current node to left sibling
		List<Router<K>> nodeRouters = node.getRouters();
		for (Router<K> router : nodeRouters) {
			leftSibling.addRouter(router);
		}
		
		// Remove the router pointing to current node from parent
		K nodeMinKey = node.getMinKey();
		parent.removeRouter(nodeMinKey);
		
		// Check if parent needs underflow handling
		if (parent.isUnderflow(this.MIN_KEYS) && !parent.equals(this.root)) {
			handleInternalNodeUnderflow(parent);
		} else if (parent.equals(this.root) && parent.size() == 0) {
			// Root has no routers, make left sibling the new root
			this.root = leftSibling;
			leftSibling.setParent(null);
		}
	}

	// Merge current node with right internal sibling
	private void mergeWithRightSiblingInternal(InternalNode<K> node, InternalNode<K> rightSibling, InternalNode<K> parent) {
		// Move all routers from right sibling to current node
		List<Router<K>> rightRouters = rightSibling.getRouters();
		for (Router<K> router : rightRouters) {
			node.addRouter(router);
		}
		
		// Remove the router pointing to right sibling from parent
		K rightMinKey = rightSibling.getMinKey();
		parent.removeRouter(rightMinKey);
		
		// Check if parent needs underflow handling
		if (parent.isUnderflow(this.MIN_KEYS) && !parent.equals(this.root)) {
			handleInternalNodeUnderflow(parent);
		} else if (parent.equals(this.root) && parent.size() == 0) {
			// Root has no routers, make current node the new root
			this.root = node;
			node.setParent(null);
		}
	}

	// private void handleInternalNodeUnderflow(InternalNode<K> node) {
	// 	InternalNode<K> parent = (InternalNode<K>) node.getParent();
	// 	if (parent.equals(null)) return;

	// 	if (parent.equals(this.root) && parent.size() == 0) {
	// 		this.root = parent.getFirstChild();
	// 		this.root.setParent(null);
	// 	}

	// 	// Find siblings and merge or borrow (simplified, not production ready)
	// 	List<BPlusTreeNode<K>> siblings = parent.getAllChildren();
	// 	int nodeIdx = siblings.indexOf(node);

	// 	if (nodeIdx > 0) {
	// 		BPlusTreeNode<K> leftSibling = siblings.get(nodeIdx - 1);
	// 		if (!leftSibling.isLeaf() && leftSibling.size() > this.MIN_KEYS) {}
	// 	}
	// }

	// Handle underflow in an internal node.
	// When a node has fewer than the minimum allowed keys, we attempt to borrow from siblings or merge.
	private void handleInternalNodeUnderflow(InternalNode<K> node) {
		InternalNode<K> parent = (InternalNode<K>) node.getParent();
		// Root node can't underflow in this context, or tree is empty, no action needed
		if (parent == null) return;

		// Check if node is the root with only one child
		if (node.equals(this.root) && node.getAllChildren().size() == 1) {
			this.root = node.getFirstChild();
			this.root.setParent(null); // Update root's parent to null (it becomes the new root)
			return;
		}

		List<BPlusTreeNode<K>> siblings = parent.getAllChildren();
		int nodeIndex = siblings.indexOf(node);

		// Try to borrow from the left sibling
		if (nodeIndex > 0) {
			InternalNode<K> leftSibling = (InternalNode<K>) siblings.get(nodeIndex - 1);
			if (!leftSibling.isLeaf() && leftSibling.size() > this.MIN_KEYS) {
				borrowRouterFromLeftSibling(node, leftSibling, parent);
				return;
			}
		}

		// Try to borrow from the right sibling if left borrowing failed
		if (nodeIndex < siblings.size() - 1) {
			InternalNode<K> rightSibling = (InternalNode<K>) siblings.get(nodeIndex + 1);
			if (!rightSibling.isLeaf() && rightSibling.size() > this.MIN_KEYS) {
				borrowRouterFromRightSibling(node, rightSibling, parent);
				return;
			}
		}

		// If borrowing fails, merge with the left sibling (or right if left sibling doesn't exist)
		if (nodeIndex > 0) {
			InternalNode<K> leftSibling = (InternalNode<K>) siblings.get(nodeIndex - 1);
			mergeWithLeftSiblingInternal(node, leftSibling, parent);
		} else if (nodeIndex < siblings.size() - 1) {
			InternalNode<K> rightSibling = (InternalNode<K>) siblings.get(nodeIndex + 1);
			mergeWithRightSiblingInternal(node, rightSibling, parent);
		}

		// Recursively propagate underflow upwards if needed
		handleInternalNodeUnderflow(parent);
	}

	/* ========================== UTILITY METHODS ====================== */

	// Get the height of the tree
	public int getHeight() {
		lock.readLock().lock();
		try {
			int height = 0;
			BPlusTreeNode<K> current = this.root;

			while (!current.isLeaf()) {
				height++;
				InternalNode<K> internal = (InternalNode<K>) current;
				current = internal.getFirstChild();
			}

			return height + 1;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public String toString() {
		return String.format("BPlusTree{order=%d, size=%d, height=%d}", this.ORDER, this.getTotalEntries(), this.getHeight());
	}
}