using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// An in-memory <see cref="IPersistentState{TState}"/> that records what was
/// asked of it, so a test can prove the grain's own state was committed - and
/// committed exactly once - independently of what the index did.
/// </summary>
/// <typeparam name="TState">The state type.</typeparam>
internal sealed class RecordingPersistentState<TState> : IPersistentState<TState>
{
    /// <summary>Initialises the state.</summary>
    /// <param name="state">The initial in-memory state.</param>
    /// <param name="recordExists">Whether storage already holds a record.</param>
    public RecordingPersistentState(TState state, bool recordExists = false)
    {
        State = state;
        RecordExists = recordExists;
    }

    /// <inheritdoc />
    public TState State { get; set; }

    /// <inheritdoc />
    public string? Etag { get; private set; }

    /// <inheritdoc />
    public bool RecordExists { get; private set; }

    /// <summary>How many times the state was committed.</summary>
    public int WriteCount { get; private set; }

    /// <summary>How many times the state was re-read from storage.</summary>
    public int ReadCount { get; private set; }

    /// <summary>How many times the stored record was deleted.</summary>
    public int ClearCount { get; private set; }

    /// <summary>An exception the next commit throws, or <c>null</c>.</summary>
    public Exception? WriteFault { get; set; }

    /// <inheritdoc />
    public Task ClearStateAsync() => ClearStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public Task ClearStateAsync(CancellationToken cancellationToken)
    {
        ClearCount++;
        RecordExists = false;
        Etag = null;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ReadStateAsync() => ReadStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public Task ReadStateAsync(CancellationToken cancellationToken)
    {
        ReadCount++;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync() => WriteStateAsync(CancellationToken.None);

    /// <inheritdoc />
    public Task WriteStateAsync(CancellationToken cancellationToken)
    {
        if (WriteFault is { } fault)
            return Task.FromException(fault);

        WriteCount++;
        RecordExists = true;
        Etag = WriteCount.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return Task.CompletedTask;
    }
}
