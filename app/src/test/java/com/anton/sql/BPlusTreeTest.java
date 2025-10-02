package com.anton.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import com.anton.storage.Slot;
import com.anton.sql.BPlusTree.Entry;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;

public class BPlusTreeTest {

	private BPlusTree<Integer> tree;
	
	@BeforeEach
	void setUp() {
		tree = new BPlusTree<>(4); // Order 4 for easier testing
	}

	/* ========================== BASIC INSERTION TESTS ====================== */

	@Test
	@DisplayName("Test insertion into empty tree")
	void testInsertIntoEmptyTree() {
		Slot slot = new Slot(1, 100);
		tree.insert(10, slot);
		
		assertEquals(slot, tree.search(10));
		assertEquals(1, tree.size());
		assertFalse(tree.isEmpty());
	}

	@Test
	@DisplayName("Test multiple insertions without split")
	void testMultipleInsertionsNoSplit() {
		tree.insert(10, new Slot(1, 100));
		tree.insert(20, new Slot(2, 200));
		tree.insert(30, new Slot(3, 300));
		
		assertEquals(3, tree.size());
		assertNotNull(tree.search(10));
		assertNotNull(tree.search(20));
		assertNotNull(tree.search(30));
	}

	@Test
	@DisplayName("Test insertion causing leaf split")
	void testInsertionWithLeafSplit() {
		// Insert enough to cause split (order 4 = max 4 keys)
		for (int i = 1; i <= 10; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		assertEquals(10, tree.size());
		
		// Verify all values are searchable
		for (int i = 1; i <= 10; i++) {
			assertNotNull(tree.search(i * 10), "Key " + (i * 10) + " should exist");
		}
	}

	@Test
	@DisplayName("Test insertion in reverse order")
	void testInsertionReverseOrder() {
		for (int i = 10; i >= 1; i--) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		assertEquals(10, tree.size());
		
		for (int i = 1; i <= 10; i++) {
			assertNotNull(tree.search(i * 10));
		}
	}

	@Test
	@DisplayName("Test insertion with duplicate keys (should replace)")
	void testInsertionWithDuplicates() {
		Slot slot1 = new Slot(1, 100);
		Slot slot2 = new Slot(2, 200);
		
		tree.insert(10, slot1);
		tree.insert(10, slot2);
		
		assertEquals(1, tree.size());
		assertEquals(slot2, tree.search(10)); // Should have the new value
	}

	@Test
	@DisplayName("Test insertion with null key throws exception")
	void testInsertNullKey() {
		assertThrows(IllegalArgumentException.class, () -> {
			tree.insert(null, new Slot(1, 100));
		});
	}

	@Test
	@DisplayName("Test insertion with null value throws exception")
	void testInsertNullValue() {
		assertThrows(IllegalArgumentException.class, () -> {
			tree.insert(10, null);
		});
	}

	/* ========================== SEARCH TESTS ====================== */

	@Test
	@DisplayName("Test search in empty tree")
	void testSearchEmptyTree() {
		assertNull(tree.search(10));
	}

	@Test
	@DisplayName("Test search for non-existent key")
	void testSearchNonExistent() {
		tree.insert(10, new Slot(1, 100));
		tree.insert(20, new Slot(2, 200));
		
		assertNull(tree.search(15));
		assertNull(tree.search(5));
		assertNull(tree.search(25));
	}

	@Test
	@DisplayName("Test search after multiple splits")
	void testSearchAfterSplits() {
		// Insert 50 items to cause multiple splits
		for (int i = 1; i <= 50; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		// Search for random keys
		assertNotNull(tree.search(1));
		assertNotNull(tree.search(25));
		assertNotNull(tree.search(50));
		assertNull(tree.search(51));
		assertNull(tree.search(0));
	}

	@Test
	@DisplayName("Test contains method")
	void testContains() {
		tree.insert(10, new Slot(1, 100));
		tree.insert(20, new Slot(2, 200));
		
		assertTrue(tree.contains(10));
		assertTrue(tree.contains(20));
		assertFalse(tree.contains(15));
	}

	/* ========================== DELETION TESTS ====================== */

	@Test
	@DisplayName("Test delete from single leaf")
	void testDeleteFromSingleLeaf() {
		tree.insert(10, new Slot(1, 100));
		tree.insert(20, new Slot(2, 200));
		
		assertTrue(tree.delete(10));
		assertEquals(1, tree.size());
		assertNull(tree.search(10));
		assertNotNull(tree.search(20));
	}

	@Test
	@DisplayName("Test delete non-existent key")
	void testDeleteNonExistent() {
		tree.insert(10, new Slot(1, 100));
		
		assertFalse(tree.delete(20));
		assertEquals(1, tree.size());
	}

	@Test
	@DisplayName("Test delete causing underflow and borrowing")
	void testDeleteWithBorrowing() {
		// Insert keys to create specific structure
		for (int i = 1; i <= 20; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		int initialSize = tree.size();
		
		// Delete some keys
		assertTrue(tree.delete(50));
		assertTrue(tree.delete(60));
		
		assertEquals(initialSize - 2, tree.size());
		assertNull(tree.search(50));
		assertNull(tree.search(60));
		
		// Verify remaining keys are still accessible
		assertNotNull(tree.search(10));
		assertNotNull(tree.search(100));
	}

	@Test
	@DisplayName("Test delete causing merge")
	void testDeleteWithMerge() {
		// Insert and then delete to cause merges
		for (int i = 1; i <= 15; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		// Delete multiple keys to trigger merges
		for (int i = 1; i <= 10; i++) {
			tree.delete(i * 10);
		}
		
		assertEquals(5, tree.size());
		
		// Verify remaining keys
		for (int i = 11; i <= 15; i++) {
			assertNotNull(tree.search(i * 10), "Key " + (i * 10) + " should still exist");
		}
	}

	@Test
	@DisplayName("Test delete all keys")
	void testDeleteAllKeys() {
		for (int i = 1; i <= 20; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		for (int i = 1; i <= 20; i++) {
			assertTrue(tree.delete(i * 10));
		}
		
		assertEquals(0, tree.size());
		assertTrue(tree.isEmpty());
	}

	@Test
	@DisplayName("Test delete with null key throws exception")
	void testDeleteNullKey() {
		assertThrows(IllegalArgumentException.class, () -> {
			tree.delete(null);
		});
	}

	/* ========================== RANGE QUERY TESTS ====================== */

	@Test
	@DisplayName("Test range query basic")
	void testRangeQueryBasic() {
		for (int i = 1; i <= 10; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		List<Entry<Integer>> results = tree.rangeQueries(20, 50);
		
		assertEquals(4, results.size()); // 20, 30, 40, 50
		assertEquals(20, results.get(0).key.intValue());
		assertEquals(50, results.get(3).key.intValue());
	}

	@Test
	@DisplayName("Test range query full range")
	void testRangeQueryFullRange() {
		for (int i = 1; i <= 10; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		List<Entry<Integer>> results = tree.rangeQueries(10, 100);
		
		assertEquals(10, results.size());
	}

	@Test
	@DisplayName("Test range query no results")
	void testRangeQueryNoResults() {
		for (int i = 1; i <= 10; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		List<Entry<Integer>> results = tree.rangeQueries(105, 200);
		
		assertEquals(0, results.size());
	}

	@Test
	@DisplayName("Test range query single element")
	void testRangeQuerySingleElement() {
		for (int i = 1; i <= 10; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		List<Entry<Integer>> results = tree.rangeQueries(50, 50);
		
		assertEquals(1, results.size());
		assertEquals(50, results.get(0).key.intValue());
	}

	@Test
	@DisplayName("Test range query with invalid range throws exception")
	void testRangeQueryInvalidRange() {
		assertThrows(IllegalArgumentException.class, () -> {
			tree.rangeQueries(50, 20);
		});
	}

	@Test
	@DisplayName("Test range query with null keys throws exception")
	void testRangeQueryNullKeys() {
		assertThrows(IllegalArgumentException.class, () -> {
			tree.rangeQueries(null, 50);
		});
		
		assertThrows(IllegalArgumentException.class, () -> {
			tree.rangeQueries(10, null);
		});
	}

	/* ========================== GET ALL ENTRIES TESTS ====================== */

	@Test
	@DisplayName("Test get all entries in empty tree")
	void testGetAllEntriesEmpty() {
		List<Entry<Integer>> entries = tree.getAllEntries();
		assertEquals(0, entries.size());
	}

	@Test
	@DisplayName("Test get all entries returns sorted order")
	void testGetAllEntriesSorted() {
		// Insert in random order
		int[] keys = {50, 20, 80, 10, 30, 70, 90, 40, 60};
		for (int key : keys) {
			tree.insert(key, new Slot(key, key * 100));
		}
		
		List<Entry<Integer>> entries = tree.getAllEntries();
		
		assertEquals(keys.length, entries.size());
		
		// Verify sorted order
		for (int i = 0; i < entries.size() - 1; i++) {
			assertTrue(entries.get(i).key < entries.get(i + 1).key);
		}
	}

	/* ========================== CLEAR TESTS ====================== */

	@Test
	@DisplayName("Test clear empty tree")
	void testClearEmpty() {
		tree.clear();
		assertTrue(tree.isEmpty());
		assertEquals(0, tree.size());
	}

	@Test
	@DisplayName("Test clear populated tree")
	void testClearPopulated() {
		for (int i = 1; i <= 20; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		assertEquals(20, tree.size());
		
		tree.clear();
		
		assertTrue(tree.isEmpty());
		assertEquals(0, tree.size());
		assertNull(tree.search(10));
	}

	@Test
	@DisplayName("Test insert after clear")
	void testInsertAfterClear() {
		tree.insert(10, new Slot(1, 100));
		tree.clear();
		tree.insert(20, new Slot(2, 200));
		
		assertEquals(1, tree.size());
		assertNull(tree.search(10));
		assertNotNull(tree.search(20));
	}

	/* ========================== SIZE AND EMPTY TESTS ====================== */

	@Test
	@DisplayName("Test size tracking")
	void testSizeTracking() {
		assertEquals(0, tree.size());
		
		tree.insert(10, new Slot(1, 100));
		assertEquals(1, tree.size());
		
		tree.insert(20, new Slot(2, 200));
		assertEquals(2, tree.size());
		
		tree.delete(10);
		assertEquals(1, tree.size());
		
		tree.delete(20);
		assertEquals(0, tree.size());
	}

	@Test
	@DisplayName("Test isEmpty")
	void testIsEmpty() {
		assertTrue(tree.isEmpty());
		
		tree.insert(10, new Slot(1, 100));
		assertFalse(tree.isEmpty());
		
		tree.delete(10);
		assertTrue(tree.isEmpty());
	}

	/* ========================== TREE HEIGHT TESTS ====================== */

	@Test
	@DisplayName("Test tree height single leaf")
	void testHeightSingleLeaf() {
		tree.insert(10, new Slot(1, 100));
		assertEquals(1, tree.getHeight());
	}

	@Test
	@DisplayName("Test tree height after splits")
	void testHeightAfterSplits() {
		int initialHeight = tree.getHeight();
		
		// Insert enough to cause multiple levels
		for (int i = 1; i <= 50; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		int finalHeight = tree.getHeight();
		assertTrue(finalHeight > initialHeight, "Tree height should increase after many insertions");
	}

	/* ========================== STRESS TESTS ====================== */

	@Test
	@DisplayName("Stress test: Sequential insertions")
	void stressTestSequentialInsertions() {
		int count = 1000;
		
		for (int i = 0; i < count; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		assertEquals(count, tree.size());
		
		// Verify all keys exist
		for (int i = 0; i < count; i++) {
			assertNotNull(tree.search(i), "Key " + i + " should exist");
		}
	}

	@Test
	@DisplayName("Stress test: Random insertions and deletions")
	void stressTestRandomOperations() {
		Random random = new Random(42); // Fixed seed for reproducibility
		Set<Integer> expectedKeys = new HashSet<>();
		
		// Insert random keys
		for (int i = 0; i < 500; i++) {
			int key = random.nextInt(10000);
			tree.insert(key, new Slot(key, key * 100));
			expectedKeys.add(key);
		}
		
		assertEquals(expectedKeys.size(), tree.size());
		
		// Delete random keys
		List<Integer> keysToDelete = new ArrayList<>(expectedKeys);
		for (int i = 0; i < 250; i++) {
			int key = keysToDelete.get(random.nextInt(keysToDelete.size()));
			tree.delete(key);
			expectedKeys.remove(key);
			keysToDelete.remove((Integer) key);
		}
		
		assertEquals(expectedKeys.size(), tree.size());
		
		// Verify remaining keys
		for (Integer key : expectedKeys) {
			assertNotNull(tree.search(key), "Key " + key + " should exist");
		}
	}

	@Test
	@DisplayName("Stress test: Large range query")
	void stressTestLargeRangeQuery() {
		for (int i = 0; i < 1000; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		List<Entry<Integer>> results = tree.rangeQueries(100, 900);
		
		assertEquals(801, results.size());
		
		// Verify order
		for (int i = 0; i < results.size() - 1; i++) {
			assertTrue(results.get(i).key < results.get(i + 1).key);
		}
	}

	@Test
	@DisplayName("Stress test: Alternating insert and delete")
	void stressTestAlternatingOperations() {
		for (int i = 0; i < 100; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		for (int i = 0; i < 50; i++) {
			tree.delete(i * 2);
		}
		
		assertEquals(50, tree.size());
		
		// Verify odd numbers remain
		for (int i = 1; i < 100; i += 2) {
			assertNotNull(tree.search(i));
		}
		
		// Verify even numbers are gone
		for (int i = 0; i < 100; i += 2) {
			assertNull(tree.search(i));
		}
	}

	/* ========================== EDGE CASE TESTS ====================== */

	@Test
	@DisplayName("Edge case: Single key operations")
	void testSingleKeyOperations() {
		tree.insert(42, new Slot(1, 100));
		assertEquals(1, tree.size());
		
		List<Entry<Integer>> all = tree.getAllEntries();
		assertEquals(1, all.size());
		
		List<Entry<Integer>> range = tree.rangeQueries(42, 42);
		assertEquals(1, range.size());
		
		tree.delete(42);
		assertTrue(tree.isEmpty());
	}

	@Test
	@DisplayName("Edge case: Minimum order tree")
	void testMinimumOrderTree() {
		BPlusTree<Integer> minTree = new BPlusTree<>(3);
		
		for (int i = 1; i <= 10; i++) {
			minTree.insert(i, new Slot(i, i * 100));
		}
		
		assertEquals(10, minTree.size());
		
		for (int i = 1; i <= 10; i++) {
			assertNotNull(minTree.search(i));
		}
	}

	@Test
	@DisplayName("Edge case: Invalid tree order throws exception")
	void testInvalidOrderThrowsException() {
		assertThrows(IllegalArgumentException.class, () -> {
			new BPlusTree<Integer>(2);
		});
		
		assertThrows(IllegalArgumentException.class, () -> {
			new BPlusTree<Integer>(1);
		});
	}

	@Test
	@DisplayName("Edge case: String keys")
	void testStringKeys() {
		BPlusTree<String> stringTree = new BPlusTree<>(4);
		
		stringTree.insert("apple", new Slot(1, 100));
		stringTree.insert("banana", new Slot(2, 200));
		stringTree.insert("cherry", new Slot(3, 300));
		
		assertNotNull(stringTree.search("banana"));
		assertEquals(3, stringTree.size());
		
		List<Entry<String>> entries = stringTree.getAllEntries();
		assertEquals("apple", entries.get(0).key);
		assertEquals("banana", entries.get(1).key);
		assertEquals("cherry", entries.get(2).key);
	}

	/* ========================== CONCURRENCY TESTS (Basic) ====================== */

	@Test
	@DisplayName("Concurrency: Multiple readers")
	void testMultipleReaders() throws InterruptedException {
		// Insert initial data
		for (int i = 0; i < 100; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		// Create multiple reader threads
		Thread[] readers = new Thread[10];
		for (int i = 0; i < readers.length; i++) {
			readers[i] = new Thread(() -> {
				for (int j = 0; j < 100; j++) {
					assertNotNull(tree.search(j));
				}
			});
			readers[i].start();
		}
		
		// Wait for all readers
		for (Thread reader : readers) {
			reader.join();
		}
		
		assertEquals(100, tree.size());
	}

	@Test
	@DisplayName("Concurrency: Reader and writer")
	void testReaderAndWriter() throws InterruptedException {
		// Insert initial data
		for (int i = 0; i < 50; i++) {
			tree.insert(i, new Slot(i, i * 100));
		}
		
		Thread writer = new Thread(() -> {
			for (int i = 50; i < 100; i++) {
				tree.insert(i, new Slot(i, i * 100));
			}
		});
		
		Thread reader = new Thread(() -> {
			for (int i = 0; i < 100; i++) {
				tree.search(i); // May or may not find newer keys
			}
		});
		
		writer.start();
		reader.start();
		
		writer.join();
		reader.join();
		
		assertEquals(100, tree.size());
	}
}