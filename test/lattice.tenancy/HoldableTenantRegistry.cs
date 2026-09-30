namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// A <see cref="TenantPolicyTestData.FakeTenantRegistry"/> whose full scan (a
/// snapshot rebuild's only registry call) can be held on a gate, so a test can
/// observe the window in which a rebuild is outstanding without racing it. Point
/// reads answer at once with the committed records and are counted. Wraps an
/// optional shared <see cref="TenantPolicyTestData.FakeTenantRegistry"/>, so one
/// silo's scans can be held while another silo reads the same records freely.
/// </summary>
internal sealed class HoldableTenantRegistry(TenantPolicyTestData.FakeTenantRegistry? inner = null) : ITenantRegistry
{
    private readonly TenantPolicyTestData.FakeTenantRegistry _inner = inner ?? new();
    private TaskCompletionSource _gate = Open();

    /// <summary>The mutable backing records.</summary>
    public List<TenantRecord> Records => _inner.Records;

    /// <summary>The number of point reads served.</summary>
    public int PointReads { get; private set; }

    /// <summary>Holds every subsequent scan until <see cref="ReleaseScans"/>.</summary>
    public void HoldScans() => _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Releases held scans.</summary>
    public void ReleaseScans() => _gate.TrySetResult();

    /// <inheritdoc />
    public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        PointReads++;
        return _inner.GetAsync(tenant, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
        _inner.ExistsAsync(tenant, cancellationToken);

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantRecord> ListAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        await foreach (var record in _inner.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return record;
        }
    }

    /// <inheritdoc />
    public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default) =>
        _inner.PutAsync(record, cancellationToken);

    /// <inheritdoc />
    public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(tenant, cancellationToken);

    private static TaskCompletionSource Open()
    {
        var gate = new TaskCompletionSource();
        gate.SetResult();
        return gate;
    }
}
