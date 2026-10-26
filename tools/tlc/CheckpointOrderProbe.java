package tlc2.tool;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import tlc2.TLCGlobals;
import tlc2.tool.fp.FPSetConfiguration;
import tlc2.tool.impl.FastTool;
import tlc2.tool.liveness.AbstractDiskGraph;
import tlc2.tool.liveness.GraphNode;
import tlc2.tool.queue.IStateQueue;
import tlc2.util.BufferedRandomAccessFile;
import tlc2.util.FP64;
import tlc2.util.NoopStateWriter;
import tlc2.value.RandomEnumerableValues;
import util.SimpleFilenameToStream;

public final class CheckpointOrderProbe extends ModelChecker {
    private CheckpointOrderProbe(ITool tool, String directory) throws Exception {
        super(tool, directory, new NoopStateWriter(), false, null, config(), System.currentTimeMillis());
    }

    private static FPSetConfiguration config() {
        FPSetConfiguration result = new FPSetConfiguration(1.0, "tlc2.tool.fp.MSBDiskFPSet");
        result.setMemory(16L * 1024 * 1024);
        return result;
    }

    private void exercise(boolean injectBeginFailure) throws Exception {
        AbstractDiskGraph graph = liveCheck.getChecker(0).getDiskGraph();
        graph.addNode(new GraphNode(1L, 0));
        IStateQueue realQueue = theStateQueue;
        AtomicBoolean resumed = new AtomicBoolean();
        theStateQueue = (IStateQueue) Proxy.newProxyInstance(IStateQueue.class.getClassLoader(),
                new Class<?>[] {IStateQueue.class}, (proxy, method, args) -> {
                    if (method.getName().equals("beginChkpt") && injectBeginFailure) {
                        throw new IOException("injected checkpoint begin failure");
                    }
                    Object result;
                    try {
                        result = method.invoke(realQueue, args);
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                    if (method.getName().equals("resumeAll")) {
                        resumed.set(true);
                        graph.addNode(new GraphNode(2L, 0));
                    }
                    return result;
                });
        if (injectBeginFailure) {
            boolean caught = false;
            try {
                checkpoint();
            } catch (IOException expected) {
                caught = expected.getMessage().equals("injected checkpoint begin failure");
            }
            if (!caught || !resumed.get()) throw new AssertionError("failed checkpoint must release suspended workers");
            if (Files.exists(Path.of(metadir, "queue.chkpt"))) {
                throw new AssertionError("failed checkpoint must not commit");
            }
            System.out.println("FAILED_BEGIN_RESUMES_WITHOUT_COMMIT");
            return;
        }
        checkpoint();
        if (!resumed.get()) throw new AssertionError("worker resume must remain present");
        long savedPointers;
        try (DataInputStream input = new DataInputStream(
                Files.newInputStream(Path.of(metadir, "dgraph_0.chkpt")))) {
            input.readLong();
            savedPointers = input.readLong();
        }
        int capturedNodes = 0;
        try (BufferedRandomAccessFile input = new BufferedRandomAccessFile(Path.of(metadir, "ptrs_0").toString(), "r")) {
            while (input.getFilePointer() < savedPointers) {
                input.readLong();
                input.readInt();
                input.readLongNat();
                capturedNodes++;
            }
        }
        System.out.println("CHECKPOINT_CAPTURED_NODES=" + capturedNodes + " LIVE_NODES=" + graph.size());
        if (capturedNodes != 1) throw new AssertionError("LIVENESS CHECKPOINT INCLUDED POST-RESUME WORK");
        if (graph.size() != 2) throw new AssertionError("controlled resumed-worker write must actually occur");
    }

    private void closeProbe() throws Exception {
        try {
            try {
                theStateQueue.finishAll();
                trace.close();
            } finally {
                liveCheck.close();
            }
        } finally {
            try {
                theFPSet.close();
            } finally {
                theFPSet.unexportObject(true);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createDirectory(Path.of(args[0]));
        FP64.Init(130);
        RandomEnumerableValues.setSeed(980653360040586054L);
        TLCGlobals.setNumWorkers(2);
        ITool tool = new FastTool(args[1], args[1],
                new SimpleFilenameToStream(Path.of(args[1]).getParent().toString()));
        for (boolean failure : new boolean[] {false, true}) {
            CheckpointOrderProbe subject = new CheckpointOrderProbe(tool,
                    Files.createDirectory(root.resolve(failure ? "failed-begin" : "normal")).toString());
            try {
                TLCGlobals.mainChecker = subject;
                subject.exercise(failure);
            } finally {
                try {
                    subject.closeProbe();
                } finally {
                    TLCGlobals.mainChecker = null;
                }
            }
        }
        System.out.println("ACTUAL_CHECKPOINT_CUT_REGRESSION_PASSED");
    }
}
