using Orleans.Concurrency;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The <see cref="ISchemaConfigEpochGrain"/> activation for one schema configuration
/// tree. Holds the epoch in memory only: each activation mints a fresh
/// <see cref="SchemaConfigEpoch.Incarnation"/>, so a reactivation reads to every
/// silo as a configuration change and no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant because neither method awaits: each reads or bumps the counter in one
/// synchronous step, so a read never queues behind another silo's advance.
/// </remarks>
[Reentrant]
internal sealed class SchemaConfigEpochGrain : Grain, ISchemaConfigEpochGrain
{
    private readonly Guid _incarnation = Guid.NewGuid();
    private long _version;

    /// <inheritdoc />
    public Task<SchemaConfigEpoch> GetAsync() =>
        Task.FromResult(new SchemaConfigEpoch(_incarnation, Interlocked.Read(ref _version)));

    /// <inheritdoc />
    public Task<SchemaConfigEpoch> AdvanceAsync() =>
        Task.FromResult(new SchemaConfigEpoch(_incarnation, Interlocked.Increment(ref _version)));
}
