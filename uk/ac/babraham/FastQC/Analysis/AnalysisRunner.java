package uk.ac.babraham.FastQC.Analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import uk.ac.babraham.FastQC.Modules.BasicStats;
import uk.ac.babraham.FastQC.Modules.QCModule;
import uk.ac.babraham.FastQC.Sequence.Sequence;
import uk.ac.babraham.FastQC.Sequence.SequenceFile;
import uk.ac.babraham.FastQC.Sequence.SequenceFormatException;

public class AnalysisRunner implements Runnable {
    private SequenceFile file;
    private QCModule[] modules;
    private List<AnalysisListener> listeners = new ArrayList<>();
    private int percentComplete = 0;
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_COUNT = 2;
    private volatile boolean isRunning = true;
    private final boolean[] isFirstRead = new boolean[] {true, true};

    // Ping-pong buffer structures
    private final List<Sequence>[] buffers;
    private final CountDownLatch[] processingLatches;

    @SuppressWarnings("unchecked")
    public AnalysisRunner(SequenceFile file) {
        this.file = file;
        this.buffers = new List[]{
            new ArrayList<>(BATCH_SIZE),
            new ArrayList<>(BATCH_SIZE)
        };
        this.processingLatches = new CountDownLatch[2];
    }

    private void resetLatch(int index) {
        processingLatches[index] = new CountDownLatch(modules != null ? modules.length : 1);
    }

    public void addAnalysisListener(AnalysisListener l) {
        if (l != null && !listeners.contains(l)) {
            listeners.add(l);
        }
    }

    public void removeAnalysisListener(AnalysisListener l) {
        if (l != null && listeners.contains(l)) {
            listeners.remove(l);
        }
    }

    public void startAnalysis(QCModule[] modules) {
        this.modules = modules;
        System.out.println("Starting analysis with " + modules.length + " modules");
        for (int i = 0; i < modules.length; i++) {
            modules[i].reset();
        }
        AnalysisQueue.getInstance().addToQueue(this);
    }

     public void run() {
        Iterator<AnalysisListener> i = listeners.iterator();
        while (i.hasNext()) {
            i.next().analysisStarted(file);
        }

        ExecutorService computeExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger processedSequences = new AtomicInteger(0);
        int currentBuffer = 0;
        long startTime = System.currentTimeMillis();
	long readTime = 0;

        try {
            while (file.hasNext() && isRunning) {
                // Clear and fill current buffer
                buffers[currentBuffer].clear();
                int sequencesRead = 0;
                while (file.hasNext() && buffers[currentBuffer].size() < BATCH_SIZE && isRunning) {
		    long readStart = System.currentTimeMillis();
                    try {
                        Sequence seq = file.next();
                        if (seq != null) {
                            buffers[currentBuffer].add(seq);
                            sequencesRead++;
                        }
                    } catch (SequenceFormatException e) {
                        notifyError(e);
                        isRunning = false;
                        break;
                    }
		    readTime += System.currentTimeMillis() - readStart;
                }

                // If we have sequences to process
                if (!buffers[currentBuffer].isEmpty()) {
                    // Store references before resetting latch
                    final List<Sequence> bufferToProcess = buffers[currentBuffer];
                    if (!isFirstRead[1 - currentBuffer]) processingLatches[1 - currentBuffer].await();
		    isFirstRead[currentBuffer] = false;
                
                    
                    // Reset latch for next round
                    resetLatch(currentBuffer);
                    final CountDownLatch currentLatch = processingLatches[currentBuffer];
                    
                    // Submit processing tasks
                    for (int moduleIndex = 0; moduleIndex < modules.length; moduleIndex++) {
                        final QCModule module = modules[moduleIndex];
                        final int moduleNum = moduleIndex;
			final int bufferIndex = currentBuffer;
                        computeExecutor.submit(() -> {
                            try {
                                for (Sequence seq : bufferToProcess) {
                                    if (!seq.isFiltered() || !module.ignoreFilteredSequences()) {
                                        module.processSequence(seq);
                                    }
                                }
                            } finally {
                                currentLatch.countDown();
                            }
                        });
                    }

                    // Update progress
                    int newCount = processedSequences.addAndGet(bufferToProcess.size());
                    updateProgress(newCount);

                    // Switch buffers
                    currentBuffer = 1 - currentBuffer;
                } else {
                    break;
                }
            }

            // Wait for final processing to complete
            processingLatches[1 - currentBuffer].await();

            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("Total processing time: " + totalTime + "ms");
            System.out.println("Total sequences processed: " + processedSequences.get());
	    System.out.println("Total read time: " + readTime);

            if (processedSequences.get() == 0) {
                handleEmptyFile();
            }

            i = listeners.iterator();
            while (i.hasNext()) {
                i.next().analysisComplete(file, modules);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            notifyError(new SequenceFormatException("Analysis interrupted"));
        } finally {
            isRunning = false;
            System.out.println("Shutting down compute executor");
            computeExecutor.shutdownNow();
            try {
                computeExecutor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void updateProgress(int seqCount) {
        if (seqCount % BATCH_SIZE == 0) {
            if (file.getPercentComplete() >= percentComplete + 5) {
                percentComplete = (((int) file.getPercentComplete()) / 5) * 5;
                Iterator<AnalysisListener> i = listeners.iterator();
                while (i.hasNext()) {
                    i.next().analysisUpdated(file, seqCount, percentComplete);
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void handleEmptyFile() {
        for (QCModule module : modules) {
            if (module instanceof BasicStats) {
                ((BasicStats) module).setFileName(file.name());
            }
        }
    }

    private void notifyError(SequenceFormatException e) {
        Iterator<AnalysisListener> i = listeners.iterator();
        while (i.hasNext()) {
            i.next().analysisExceptionReceived(file, e);
        }
    }
}
