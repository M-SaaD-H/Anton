package com.anton.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// maintains mapping of page numbers -> page objects
public class PageManager implements AutoCloseable {
  private final FileManager fileManager;
  private final Map<Integer, Page> pageCache = new HashMap<>();

  public PageManager(FileManager fileManager) {
    this.fileManager = fileManager;
  }

  public Page getPage(int pageNumber) throws IOException {
    // If page already exists
    if (pageCache.containsKey(pageNumber)) {
      return pageCache.get(pageNumber);
    }

    // else create a new page
    Page page = new Page();
    page.readFromFile(fileManager, pageNumber);
    pageCache.put(pageNumber, page);
    return page;
  }

  public void writePage(int pageNumber, Page page) throws IOException {
    page.writeToFile(fileManager, pageNumber);
    if (pageCache.containsKey(pageNumber)) {
      pageCache.put(pageNumber, page);
    }
  }

  public Page allocateNewPage() {
    Page page = new Page();
    int newPageNumber = pageCache.size(); // will improve this further
    pageCache.put(newPageNumber, page);
    return page;
  }

  public int getNumOfPages() {
    return this.pageCache.size();
  }

  public void flushPage(int pageNumber) throws IOException {
    Page page = pageCache.get(pageNumber);
    if (page != null) {
      page.writeToFile(fileManager, pageNumber);
    }
  }

  @Override
  public void close() throws IOException {
    if (fileManager != null) {
      fileManager.close();
    }
  }
}
