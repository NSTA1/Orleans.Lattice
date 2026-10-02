using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Schema.Tests.Fakes;

/// <summary>
/// In-memory <see cref="IPersistentState{TState}"/> for unit-testing POCO grains
/// without a storage provider. Tracks the write count and can be primed to throw
/// on the next write to exercise the coordinator's rollback-on-write-failure path.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    public T State { get; set; } = new();
    public string Etag => string.Empty;
    public bool RecordExists { get; private set; } = true;

    /// <summary>Number of successful <see cref="WriteStateAsync"/> calls.</summary>
    public int WriteCount { get; private set; }

    /// <summary>
    /// When set, a <see cref="WriteStateAsync"/> throws this exception instead of
    /// persisting, then clears itself so the following write succeeds.
    /// </summary>
    /// <remarks>
    /// Pair it with <see cref="ThrowOnWriteNumber"/> when the write under test is
    /// not the first one a call performs. Several coordinator entry points persist
    /// an alias reservation before they reach the write whose rollback is being
    /// exercised, so a one-shot fault that always fires first lands in the
    /// reservation instead - and the test still passes, because the assertion it
    /// makes is equally true of the earlier abort. That is a silent false green,
    /// and the ordinal is what closes it.
    /// </remarks>
    public Exception? ThrowOnWrite { get; set; }

    /// <summary>
    /// The 1-based ordinal of the <see cref="WriteStateAsync"/> call that
    /// <see cref="ThrowOnWrite"/> fires on. Defaults to the first write.
    /// </summary>
    public int ThrowOnWriteNumber { get; set; } = 1;

    /// <summary>Total <see cref="WriteStateAsync"/> attempts, successful or not.</summary>
    public int WriteAttempts { get; private set; }

    /// <summary>
    /// When set, invoked with the 1-based attempt ordinal at the start of every
    /// <see cref="WriteStateAsync"/> and awaited before the write completes, so a
    /// test can hold a specific write open on a <see cref="TaskCompletionSource"/>
    /// and observe what an interleaved read sees meanwhile. <c>null</c> (the
    /// default) keeps every write synchronous.
    /// </summary>
    public Func<int, Task>? BeforeWrite { get; set; }

    public Task ClearStateAsync()
    {
        State = new();
        RecordExists = false;
        return Task.CompletedTask;
    }

    public Task ReadStateAsync() => Task.CompletedTask;

    public Task WriteStateAsync()
    {
        WriteAttempts++;
        if (BeforeWrite is { } hook)
        {
            return HeldWriteAsync(hook, WriteAttempts);
        }

        return CompleteWrite(WriteAttempts);
    }

    private async Task HeldWriteAsync(Func<int, Task> hook, int attempt)
    {
        await hook(attempt);
        await CompleteWrite(attempt);
    }

    private Task CompleteWrite(int attempt)
    {
        if (ThrowOnWrite is { } ex && attempt == ThrowOnWriteNumber)
        {
            ThrowOnWrite = null;
            throw ex;
        }

        WriteCount++;
        RecordExists = true;
        return Task.CompletedTask;
    }
}
