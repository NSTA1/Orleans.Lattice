namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// An <see cref="IFileWalFileSystem"/> that records every write, flush, and
/// segment replace a <see cref="FileWalShard"/> issues, in the order issued,
/// and can hold or fault the next physical flush (<c>Flush(true)</c>). It lets
/// a test observe <i>when</i> the shard fsyncs relative to acknowledging an
/// operation, which no same-process reopen can (issue #3462): unsynced bytes
/// are still in the OS page cache when a provider is rebuilt.
/// </summary>
internal sealed class RecordingFileWalFileSystem : IFileWalFileSystem
{
    private readonly object _lock = new();
    private readonly List<IoEvent> _events = new();
    private int _faultsRemaining;
    private Hold? _armedHold;

    /// <summary>The kind of a recorded I/O event.</summary>
    internal enum IoKind
    {
        /// <summary>Bytes handed to the stream.</summary>
        Write,

        /// <summary>A <c>Flush(false)</c>: bytes pushed to the OS, not to the device.</summary>
        Flush,

        /// <summary>A <c>Flush(true)</c> that completed: bytes forced to the device.</summary>
        FlushToDisk,

        /// <summary>A <c>Flush(true)</c> this recorder faulted.</summary>
        FlushToDiskFaulted,

        /// <summary>The live segment file was replaced by a compaction target.</summary>
        Replace,
    }

    /// <summary>Which file a recorded event touched.</summary>
    internal enum IoTarget
    {
        /// <summary>The live <c>wal.log</c> segment file.</summary>
        Log,

        /// <summary>The temporary file a compaction rewrites into.</summary>
        CompactionTarget,
    }

    /// <summary>One recorded event.</summary>
    internal readonly record struct IoEvent(IoKind Kind, IoTarget Target);

    /// <summary>
    /// A one-shot barrier engaged by the next <c>Flush(true)</c>: the flushing
    /// thread signals <see cref="Entered"/> and then blocks until
    /// <see cref="Release"/> is called.
    /// </summary>
    internal sealed class Hold
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _release = new(false);

        /// <summary>Completes once a flush has entered the hold.</summary>
        public Task Entered => _entered.Task;

        /// <summary>Lets the held flush proceed.</summary>
        public void Release() => _release.Set();

        internal void Engage()
        {
            _entered.TrySetResult();
            _release.Wait();
        }
    }

    /// <summary>A snapshot of every event recorded so far, in issue order.</summary>
    public IReadOnlyList<IoEvent> Events
    {
        get
        {
            lock (_lock)
            {
                return _events.ToArray();
            }
        }
    }

    /// <summary>
    /// Arms a hold on the next <c>Flush(true)</c>. The caller must release it,
    /// normally in a <c>finally</c>, or the flushing thread stays blocked.
    /// </summary>
    public Hold ArmHold()
    {
        var hold = new Hold();
        lock (_lock)
        {
            _armedHold = hold;
        }

        return hold;
    }

    /// <summary>
    /// Faults the next <paramref name="count"/> <c>Flush(true)</c> calls. Each
    /// faulted call first pushes the bytes to the OS with <c>Flush(false)</c>
    /// and then throws <see cref="IOException"/>, modelling an fsync that
    /// reports EIO after the kernel already holds the bytes.
    /// </summary>
    public void ArmFault(int count = 1)
    {
        lock (_lock)
        {
            _faultsRemaining = count;
        }
    }

    /// <inheritdoc />
    public FileStream OpenLog(string path) =>
        new RecordingFileStream(this, IoTarget.Log, path, FileMode.OpenOrCreate, FileAccess.ReadWrite);

    /// <inheritdoc />
    public FileStream CreateCompactionTarget(string path) =>
        new RecordingFileStream(this, IoTarget.CompactionTarget, path, FileMode.Create, FileAccess.Write);

    /// <inheritdoc />
    public void ReplaceLog(string compactedPath, string logPath)
    {
        Record(IoKind.Replace, IoTarget.Log);
        System.IO.File.Move(compactedPath, logPath, overwrite: true);
    }

    private void Record(IoKind kind, IoTarget target)
    {
        lock (_lock)
        {
            _events.Add(new IoEvent(kind, target));
        }
    }

    /// <summary>Returns <see langword="true"/> when this flush must fault.</summary>
    private bool BeforeFlushToDisk()
    {
        Hold? hold;
        lock (_lock)
        {
            if (_faultsRemaining > 0)
            {
                _faultsRemaining--;
                return true;
            }

            hold = _armedHold;
            _armedHold = null;
        }

        hold?.Engage();
        return false;
    }

    private sealed class RecordingFileStream(
        RecordingFileWalFileSystem owner,
        IoTarget target,
        string path,
        FileMode mode,
        FileAccess access)
        : FileStream(path, mode, access, FileShare.None)
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            owner.Record(IoKind.Write, target);
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            owner.Record(IoKind.Write, target);
            base.Write(buffer);
        }

        public override void Flush(bool flushToDisk)
        {
            if (!flushToDisk)
            {
                base.Flush(false);
                owner.Record(IoKind.Flush, target);
                return;
            }

            if (owner.BeforeFlushToDisk())
            {
                base.Flush(false);
                owner.Record(IoKind.FlushToDiskFaulted, target);
                throw new IOException("Simulated fsync failure (EIO) injected by the test recorder.");
            }

            base.Flush(true);
            owner.Record(IoKind.FlushToDisk, target);
        }
    }
}
