package org.example;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class CompletableFutureForkJoinDemo {

    public static void main(String[] args) throws IOException {
        // Read all lines from your file
        List<String> lines = Files.readAllLines(Paths.get(
                "/Users/sbombatkar/IdeaProjects/Parallel_File_Processor/src/main/java/org/example/data"
        ));

        ExecutorService executor = ForkJoinPool.commonPool();

        // Decide chunk size (e.g., 100 lines per chunk)
        int chunkSize = 100;
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < lines.size(); i += chunkSize) {
            chunks.add(lines.subList(i, Math.min(i + chunkSize, lines.size())));
        }

        // Launch parallel tasks with chunk index tracking
        List<CompletableFuture<ChunkResult>> futures = IntStream.range(0, chunks.size())
                .mapToObj(chunkIndex -> CompletableFuture.supplyAsync(() -> {
                    List<String> processed = new ArrayList<>();
                    for (String line : chunks.get(chunkIndex)) {
                        processed.add(Thread.currentThread().getName() + " -> " + line);
                    }
                    return new ChunkResult(chunkIndex, processed);
                }, executor))
                .toList();

        // Wait for all tasks and sort by chunk index
        List<ChunkResult> results = futures.stream()
                .map(CompletableFuture::join)
                .sorted(Comparator.comparingInt(r -> r.index))
                .toList();

        // Print in correct order
        results.forEach(result -> result.lines.forEach(System.out::println));

        executor.shutdown();
    }

    // Helper class to store chunk index + processed lines
    static class ChunkResult {
        int index;
        List<String> lines;
        ChunkResult(int index, List<String> lines) {
            this.index = index;
            this.lines = lines;

        }
    }
}
