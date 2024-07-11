package org.example.structs;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NonblockingSet <T> implements Set<T> {
    private final Node<T> head;

    public NonblockingSet() {
        head = new Node<>(Integer.MIN_VALUE);
        Node<T> tail = new Node<>(Integer.MAX_VALUE);
        tail.next = new AtomicMarkableReference<>(null, false);
        head.next = new AtomicMarkableReference<>(tail, false);
    }

    @Override
    public boolean add(T item) {
        int key = item.hashCode();
        while (true) {
            Window<T> window = Window.find(head, key);
            Node<T> pred = window.pred, curr = window.curr;
            if (curr.key == key) {
                return false;
            }
            Node<T> node = new Node<>(item);
            node.next = new AtomicMarkableReference<>(curr, false);
            if (pred.next.compareAndSet(curr, node, false, false)) {
                return true;
            }

        }
    }

    @Override
    public boolean remove(T item) {
        int key = item.hashCode();
        boolean snip;
        while (true) {
            Window<T> window = Window.find(head, key);
            Node<T> pred = window.pred, curr = window.curr;
            if (curr.key != key) {
                return false;
            }
            Node<T> succ = curr.next.getReference();
            snip = curr.next.compareAndSet(succ, succ, false, true);
            if(!snip){
                continue;
            }
            pred.next.compareAndSet(curr, succ, false, false);
            return true;
        }
    }

    @Override
    public boolean contains(T item) {
        int key = item.hashCode();
        Node<T> curr = head;
        while (curr.key < key) {
            curr = curr.next.getReference();
        }
        return curr.key == key && !curr.next.isMarked();
    }


    private static class Window<T> {
        final Node<T> pred, curr;

        private Window(final Node<T> pred, final Node<T> curr) {
            this.pred = pred;
            this.curr = curr;
        }


        static <T> Window<T> find(Node<T> head, int key) {
            Node<T> pred, curr, succ;
            boolean[] marked = new boolean[1];
            boolean snip;


            retry:
            while (true) {
                pred = head;
                curr = pred.next.getReference();
                while (true) {
                    succ = curr.next.get(marked);
                    while (marked[0]) {
                        snip = pred.next.compareAndSet(curr, succ, false, false);
                        if (!snip) {
                            continue retry;
                        }
                        curr = succ;
                        succ = curr.next.get(marked);
                    }
                    if (curr.key >= key) {
                        return new Window<>(pred, curr);
                    }
                    pred = curr;
                    curr = succ;
                }
            }
        }
    }

    private static class Node<T> {
        T item;
        final int key;

        AtomicMarkableReference<Node<T>> next;
        Lock lock = new ReentrantLock();

        public Node(final int key) {
            this.key = key;
        }

        public Node(final T item) {
            this.item = item;
            this.key = item.hashCode();
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }
    }
}
