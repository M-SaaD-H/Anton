package com.anton.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.anton.storage.Slot;

public class BPlusTree<K extends Comparable<K>> {
	// max number of keys per node
	private final int ORDER;
	// min number keys per node
	private final int MIN_KEYS;
	// root of the B+ Tree
	private final BPlusTreeNode<K> root;

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
		public LeafNode<K> getNext() { return this.next; }
		public LeafNode<K> getPrevious() { return this.previous; }
		public void setNext(LeafNode<K> next) { this.next = next; }
		public void setPrevious(LeafNode<K> previous) { this.previous = previous; }
		
		@Override
		public String toString() {
			return String.format("LeafNode{size=%d, entries=%s}", this.entries.size(), this.entries);
		}
	}

	// public BPlusTree() {
	// 	this(3); // default order
	// }

	// public BPlusTree(int order) {
	// 	if (order < 3) {
	// 		throw new IllegalArgumentException("Order must be at least 3.");
	// 	}
	// 	this.ORDER = order;
	// 	this.MIN_KEYS = (order - 1) / 2;
	// 	this.root = new LeafNode<>();
	// }

	// find appropriate leaf node for the given key
	// private Slot findLeaf(T key) {
	// return this.root.keys.get(key);
	// }
}