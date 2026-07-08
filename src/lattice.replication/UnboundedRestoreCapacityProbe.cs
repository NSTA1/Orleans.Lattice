using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="IRestoreCapacityProbe"/>: admits every target. This is
/// the correct behaviour when no storage or memory budget is configured - the
/// restore engine's own per-leaf memory, concurrency, and shedding limits still
/// apply during the build, so an infeasible target that slips past admission is
/// caught (more expensively) mid-build and votes to abort. A host that wants a
/// cheap up-front refusal registers its own <see cref="IRestoreCapacityProbe"/>
/// singleton that measures real headroom against
/// <see cref="RestoreAdmissionReport.TotalByteLength"/> and
/// <see cref="RestoreAdmissionReport.ShardCount"/>.
/// </summary>
internal sealed class UnboundedRestoreCapacityProbe : IRestoreCapacityProbe
{
    /// <inheritdoc />
    public Task<bool> CanHostAsync(RestoreAdmissionReport report, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        return Task.FromResult(true);
    }
}
