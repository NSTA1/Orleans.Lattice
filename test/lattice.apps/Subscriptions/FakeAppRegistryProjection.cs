namespace Orleans.Lattice.Apps.Tests;

/// <summary>A settable <see cref="IAppRegistryProjection"/> for subscription runtime tests.</summary>
internal sealed class FakeAppRegistryProjection : IAppRegistryProjection
{
    private CompiledAppRegistrySnapshot _current = CompiledAppRegistrySnapshot.Empty;

    public int EnsureWarmCalls { get; private set; }

    public long CurrentEpoch => Current.Epoch;

    public CompiledAppRegistrySnapshot Current => Volatile.Read(ref _current);

    /// <summary>Publishes a new snapshot of <paramref name="records"/> at the next epoch.</summary>
    public CompiledAppRegistrySnapshot Publish(params AppRegistryRecord[] records)
    {
        var next = CompiledAppRegistrySnapshot.Compile(records, Current.Epoch + 1);
        Volatile.Write(ref _current, next);
        return next;
    }

    public Task EnsureWarmAsync(CancellationToken cancellationToken = default)
    {
        EnsureWarmCalls++;
        return Task.CompletedTask;
    }
}
