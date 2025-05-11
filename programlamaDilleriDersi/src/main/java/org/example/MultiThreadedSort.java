package org.example;

import java.util.*;
import java.util.concurrent.*;

public class MultiThreadedSort {

    static int[] array;
    static int[][] sortedParts;
    static int threadCount;
    static CountDownLatch latch;
    static Map<Integer, Integer> assignedThreads = new LinkedHashMap<>(); // sıralı tutar

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int size = 1000;
        array = new int[size];

        System.out.print("Thread sayısı: ");
        threadCount = scanner.nextInt();
        sortedParts = new int[threadCount][];

        // Diziyi random doldur
        Random rand = new Random();
        for (int i = 0; i < size; i++) {
            array[i] = rand.nextInt(1000);
        }

        System.out.println("Orijinal Dizi: " + Arrays.toString(array));

        // Thread atamaları alınır
        Set<Integer> kullanılanThreadler = new HashSet<>();
        for (int i = 0; i < threadCount; i++) {
            boolean assigned = false;
            while (!assigned) {
                System.out.println("\nMevcut boşta olan thread’ler: " + getAvailableThreads(threadCount, kullanılanThreadler));
                System.out.print("Parça " + (i + 1) + " için atanacak thread numarası: ");
                String input = scanner.next();

                try {
                    int tId = Integer.parseInt(input.trim());
                    if (tId < 0 || tId >= threadCount) {
                        System.out.println("Geçersiz thread numarası.");
                    } else if (kullanılanThreadler.contains(tId)) {
                        System.out.println("Thread " + tId + " zaten başka bir parçaya atandı.");
                    } else {
                        assignedThreads.put(tId, i); // threadId → parçaIndex
                        kullanılanThreadler.add(tId);
                        System.out.println("Thread " + tId + " → Parça " + (i + 1) + " ATANDI.");
                        assigned = true;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Geçersiz sayı girdiniz.");
                }
            }
        }

        // İlk atanan thread koordinatör olacak
        int koordinatorId = assignedThreads.keySet().iterator().next();
        latch = new CountDownLatch(threadCount - 1); // koordinatör hariç

        // Koordinatör thread başlatılır
        new Thread(new CoordinatorThread(koordinatorId)).start();
    }

    // Koordinatör thread: parçalama, dağıtım, kendi sıralama ve merge
    static class CoordinatorThread implements Runnable {
        int koordinatorId;

        CoordinatorThread(int koordinatorId) {
            this.koordinatorId = koordinatorId;
        }

        @Override
        public void run() {
            int size = array.length;
            int partSize = size / threadCount;

            for (Map.Entry<Integer, Integer> entry : assignedThreads.entrySet()) {
                int threadId = entry.getKey();
                int partIndex = entry.getValue();
                int start = partIndex * partSize;
                int end = (partIndex == threadCount - 1) ? array.length : start + partSize;

                if (threadId == koordinatorId) {
                    int[] part = Arrays.copyOfRange(array, start, end);
                    Arrays.sort(part);
                    sortedParts[partIndex] = part;
                    System.out.println("Thread " + threadId + " (koordinatör) parça " + (partIndex + 1) + " sıralandı.");
                } else {
                    new Thread(new SortTask(threadId, partIndex, start, end)).start();
                }
            }

            try {
                latch.await(); // diğerlerinin bitmesini bekle
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Parçaları yazdır
            for (int i = 0; i < threadCount; i++) {
                System.out.println("Parça " + (i + 1) + ": " + Arrays.toString(sortedParts[i]));
            }

            // Merge
            int[] finalSorted = mergeAll(sortedParts);
            System.out.println("\nTümleştirilmiş Sıralı Dizi: " + Arrays.toString(finalSorted));
        }
    }

    // Diğer thread'lerin sıralama görevi
    static class SortTask implements Runnable {
        int threadId, partIndex, start, end;

        SortTask(int threadId, int partIndex, int start, int end) {
            this.threadId = threadId;
            this.partIndex = partIndex;
            this.start = start;
            this.end = end;
        }

        public void run() {
            int[] part = Arrays.copyOfRange(array, start, end);
            Arrays.sort(part);
            sortedParts[partIndex] = part;
            System.out.println("Thread " + threadId + " parça " + (partIndex + 1) + " sıralandı.");
            latch.countDown();
        }
    }

    // Merge işlemi
    static int[] mergeAll(int[][] parts) {
        PriorityQueue<Element> minHeap = new PriorityQueue<>();
        int totalSize = 0;
        int[] indexes = new int[parts.length];

        for (int i = 0; i < parts.length; i++) {
            if (parts[i] != null && parts[i].length > 0) {
                minHeap.add(new Element(parts[i][0], i));
                totalSize += parts[i].length;
            }
        }

        int[] result = new int[totalSize];
        int idx = 0;

        while (!minHeap.isEmpty()) {
            Element min = minHeap.poll();
            result[idx++] = min.value;

            if (++indexes[min.arrayIndex] < parts[min.arrayIndex].length) {
                minHeap.add(new Element(parts[min.arrayIndex][indexes[min.arrayIndex]], min.arrayIndex));
            }
        }

        return result;
    }

    // Merge için yardımcı sınıf
    static class Element implements Comparable<Element> {
        int value, arrayIndex;

        Element(int value, int arrayIndex) {
            this.value = value;
            this.arrayIndex = arrayIndex;
        }

        public int compareTo(Element other) {
            return Integer.compare(this.value, other.value);
        }
    }

    private static List<Integer> getAvailableThreads(int threadCount, Set<Integer> kullanılanlar) {
        List<Integer> kalan = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            if (!kullanılanlar.contains(i)) {
                kalan.add(i);
            }
        }
        return kalan;
    }
}
