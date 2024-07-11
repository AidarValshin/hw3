package org.example;

import lombok.AllArgsConstructor;
import org.example.structs.NonblockingSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.join;

public class MultiThreadCrawler {
    private Queue<Node> searchQueue = new ConcurrentLinkedQueue<>();

    private org.example.structs.Set<String> visited = new NonblockingSet<>();

    private WikiClient client = new WikiClient();
    private final int THREADS_COUNT = 4;
    private ExecutorService executorService = Executors.newFixedThreadPool(THREADS_COUNT);
    private AtomicBoolean isFinished = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        MultiThreadCrawler crawler = new MultiThreadCrawler();

        long startTime = System.nanoTime();
        String result = crawler.find("Java_(programming_language)", "Time_series", 5, TimeUnit.MINUTES);
        long finishTime = TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        System.out.println("Took " + finishTime + " seconds, result is: " + result);

    }

    public String find(String from, String target, long timeout, TimeUnit timeUnit) throws Exception {
        long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        searchQueue.offer(new Node(from, null));
        Node result = null;
        while (!searchQueue.isEmpty() && !isFinished.get()) {
            while (searchQueue.size() == 1) {
                if (deadline < System.nanoTime()) {
                    try {
                        if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executorService.shutdownNow();
                    }
                    throw new TimeoutException();
                }
                Node node = searchQueue.poll();
                System.out.println("Get page: " + node.title);
                Set<String> links = client.getByTitle(node.title);
                if (links.isEmpty()) {
                    //pageNotFound
                    continue;
                }
                for (String link : links) {
                    String currentLink = link.toLowerCase();
                    if (visited.contains(currentLink)) {
                        continue;
                    }
                    visited.add(currentLink);
                    Node subNode = new Node(link, node);
                    if (target.equalsIgnoreCase(currentLink)) {
                        result = subNode;
                        break;
                    }
                    searchQueue.offer(subNode);
                }


                if (result != null) {
                    List<String> resultList = new ArrayList<>();
                    Node search = result;
                    while (true) {
                        resultList.add(search.title);
                        if (search.next == null) {
                            break;
                        }
                        search = search.next;
                    }
                    Collections.reverse(resultList);
                    isFinished.compareAndSet(false, true);
                    return join(" > ", resultList);
                }
            }

            ArrayList<Future<String>> futures = new ArrayList<>();

            for (int i = 0; i < THREADS_COUNT; i++) {
                futures.add(executorService.submit(() -> deepSearch( target)));
            }

            while (!futures.stream().allMatch(Future::isDone)) {
                for (Future<String> f : futures) {
                    if (!f.isDone()) {
                        continue;
                    }
                    try {
                        String resultPath = f.get();
                        if (resultPath != null) {
                            isFinished.setRelease(true);
                            try {
                                if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                                    executorService.shutdownNow();
                                }
                            } catch (InterruptedException e) {
                                executorService.shutdownNow();
                            }
                            return resultPath;
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        executorService.shutdownNow();
        return "not found";
    }

    public String deepSearch( String target) {
        Node result = null;

        Node node = searchQueue.poll();
        if (node == null) {
            return null;
        }
        if (isFinished.get()) {
            return null;
        }
        Set<String> links = Collections.emptySet();
        boolean isLinksGot = false;
        int count = 0;
        System.out.println("Get page: " + node.title + "on thread " + Thread.currentThread());
        while (count < 3 && !isLinksGot) {
            try {
                count++;
                links = client.getByTitle(node.title);
                isLinksGot = true;
            } catch (IOException e) {
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        if (links.isEmpty()) {
            //pageNotFound
            return null;
        }
        if (isFinished.get()) {
            return null;
        }
        for (String link : links) {
            String currentLink = link.toLowerCase();
            if (visited.contains(currentLink)) {
                continue;
            }
            visited.add(currentLink);
            Node subNode = new Node(link, node);
            if (target.equalsIgnoreCase(currentLink)) {
                result = subNode;
                isFinished.setRelease(true);
                break;
            }
            searchQueue.offer(subNode);
        }

        if (result != null) {
            List<String> resultList = new ArrayList<>();
            Node search = result;
            while (true) {
                resultList.add(search.title);
                if (search.next == null) {
                    break;
                }
                search = search.next;
            }
            Collections.reverse(resultList);

            return join(" > ", resultList);
        }

        if (isFinished.get()) {
            return null;
        }
        String path = null;

        while (!isFinished.get() && path == null && !searchQueue.isEmpty()) {
            path = deepSearch( target);
        }
        return path;
    }

    @AllArgsConstructor
    private static class Node {
        String title;
        Node next;
    }
}
