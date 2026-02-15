    package io.github.coderodde.util;

    /**
     * This class implements a simple heap for holding key/record indices.
     */
    class Heap {
        private static final int DEFAULT_CAPACITY = 1024;

        private int[] keys       = new int[DEFAULT_CAPACITY];
        private int[] runIndices = new int[DEFAULT_CAPACITY];
        private int size;

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        int topKey() {
            return keys[0];
        }

        int topRunIndex() {
            return runIndices[0];
        }

        void removeTop() {
            if (--size == 0) {
                return;
            }

            keys[0] = keys[size];
            runIndices[0] = runIndices[size];
            siftDown();
        }

        void insert(int key, int runIndex) {
            ensureCapacity();
            keys[size] = key;
            runIndices[size] = runIndex;
            ++size;
            siftUp();
        }

        private void siftUp() {
            int i = size - 1;

            while (i > 0) {
                int parent = (i - 1) >>> 1;

                if (keys[parent] <= keys[i]) {
                    return;
                }

                swap(parent, i);
                i = parent;
            }
        }

        private void siftDown() {
            int i = 0;

            while (true) {
                int left = (i << 1) + 1;

                if (left >= size) {
                    return;
                }

                int right = left + 1;
                int smallest = left;

                if (right < size && keys[left] > keys[right]) {
                    smallest = right;
                }

                if (keys[i] <= keys[smallest]) {
                    return;
                }

                swap(i, smallest);
                i = smallest;
            }
        }

        private void swap(int i, int j) {
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp;

            tmp = runIndices[i];
            runIndices[i] = runIndices[j];
            runIndices[j] = tmp;
        }

        private void ensureCapacity() {
            if (size == keys.length) {
                int newCapacity = 2 * size;
                int[] newKeys       = new int[newCapacity];
                int[] newRunIndices = new int[newCapacity];

                System.arraycopy(keys,
                                 0, 
                                 newKeys, 
                                 0,
                                 size);

                System.arraycopy(runIndices, 
                                 0,
                                 newRunIndices,
                                 0, 
                                 size);

                keys       = newKeys;
                runIndices = newRunIndices;
            }
        }
    }
