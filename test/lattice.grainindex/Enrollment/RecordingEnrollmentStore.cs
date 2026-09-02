using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// An in-memory <see cref="IGrainIndexEnrollmentStore"/> that records the exact
/// sequence of operations, so a test can assert the ordering the outbox depends
/// on rather than only its end state.
/// </summary>
internal sealed class RecordingEnrollmentStore : IGrainIndexEnrollmentStore
{
    /// <summary>The operations performed, in order, as <c>verb:index/grain</c>.</summary>
    public List<string> Log { get; } = [];

    /// <summary>The seen markers, keyed as <c>index/grain</c>.</summary>
    public Dictionary<string, GrainIndexEnrollmentRecord> Enrollments { get; } = new(StringComparer.Ordinal);

    /// <summary>The outstanding outbox entries, keyed as <c>index/grain</c>.</summary>
    public Dictionary<string, GrainIndexPendingProjection> Pending { get; } = new(StringComparer.Ordinal);

    /// <summary>An exception every read throws, or <c>null</c>.</summary>
    public Exception? ReadFault { get; set; }

    /// <summary>An exception every <see cref="ScanPendingAsync"/> throws before yielding, or <c>null</c>.</summary>
    public Exception? ScanFault { get; set; }

    /// <summary>An exception every outbox write throws, or <c>null</c>.</summary>
    public Exception? WritePendingFault { get; set; }

    /// <summary>An exception every completion throws, or <c>null</c>.</summary>
    public Exception? CompleteFault { get; set; }

    private int _scanCount;

    /// <summary>
    /// Completes the first time the outbox is scanned, so a test can wait for a
    /// background pass to happen instead of guessing how long it takes.
    /// </summary>
    public TaskCompletionSource ScanObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Completes the second time the outbox is scanned, guaranteeing that the
    /// periodic timer has fired at least once between the first and second pass.
    /// </summary>
    public TaskCompletionSource SecondScanObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <inheritdoc />
    public Task<GrainIndexEnrollmentRecord?> ReadEnrollmentAsync(
        string indexName,
        string grainKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);
        Log.Add($"read:{indexName}/{grainKey}");

        if (ReadFault is { } fault)
            return Task.FromException<GrainIndexEnrollmentRecord?>(fault);

        return Task.FromResult(
            Enrollments.TryGetValue(Compose(indexName, grainKey), out var record) ? record : null);
    }

    /// <inheritdoc />
    public Task WritePendingAsync(GrainIndexPendingProjection pending, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pending);
        Log.Add($"pending:{pending.IndexName}/{pending.GrainKey}");

        if (WritePendingFault is { } fault)
            return Task.FromException(fault);

        Pending[Compose(pending.IndexName, pending.GrainKey)] = pending;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CompleteAsync(
        string indexName,
        string grainKey,
        GrainIndexProjection projection,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(projection);
        Log.Add($"complete:{indexName}/{grainKey}");

        if (CompleteFault is { } fault)
            return Task.FromException(fault);

        var composed = Compose(indexName, grainKey);
        Enrollments[composed] = new GrainIndexEnrollmentRecord(projection);
        Pending.Remove(composed);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WithdrawAsync(string indexName, string grainKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);
        Log.Add($"withdraw:{indexName}/{grainKey}");

        var composed = Compose(indexName, grainKey);
        Enrollments.Remove(composed);
        Pending.Remove(composed);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ScanSeenKeysAsync(
        string indexName,
        string firstKeyInclusive,
        string lastKeyInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(firstKeyInclusive);
        ArgumentNullException.ThrowIfNull(lastKeyInclusive);
        Log.Add($"scanseen:{indexName}/{firstKeyInclusive}..{lastKeyInclusive}");

        var prefix = $"{indexName}/";
        var matches = Enrollments.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .Select(k => k[prefix.Length..])
            .Where(k =>
                string.CompareOrdinal(k, firstKeyInclusive) >= 0
                && string.CompareOrdinal(k, lastKeyInclusive) <= 0)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        foreach (var grainKey in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return grainKey;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<GrainIndexPendingProjection> ScanPendingAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Log.Add("scan");
        ScanObserved.TrySetResult();
        _scanCount++;
        if (_scanCount >= 2)
            SecondScanObserved.TrySetResult();

        if (ScanFault is { } fault)
            throw fault;

        foreach (var pending in Pending.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Value).ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return pending;
            await Task.Yield();
        }
    }

    /// <summary>Seeds a confirmed enrolment, as a previous activation would have left it.</summary>
    /// <param name="indexName">The index name.</param>
    /// <param name="grainKey">The encoded grain key.</param>
    /// <param name="projection">The projection the index holds.</param>
    public void SeedEnrollment(string indexName, string grainKey, GrainIndexProjection projection) =>
        Enrollments[Compose(indexName, grainKey)] = new GrainIndexEnrollmentRecord(projection);

    /// <summary>Whether an outbox entry is outstanding.</summary>
    /// <param name="indexName">The index name.</param>
    /// <param name="grainKey">The encoded grain key.</param>
    /// <returns><c>true</c> when an entry is outstanding.</returns>
    public bool HasPending(string indexName, string grainKey) =>
        Pending.ContainsKey(Compose(indexName, grainKey));

    /// <summary>Whether a seen marker exists.</summary>
    /// <param name="indexName">The index name.</param>
    /// <param name="grainKey">The encoded grain key.</param>
    /// <returns><c>true</c> when the grain is enrolled.</returns>
    public bool IsEnrolled(string indexName, string grainKey) =>
        Enrollments.ContainsKey(Compose(indexName, grainKey));

    private static string Compose(string indexName, string grainKey) => $"{indexName}/{grainKey}";
}
