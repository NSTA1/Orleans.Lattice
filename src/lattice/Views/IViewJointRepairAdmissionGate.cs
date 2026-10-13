namespace Orleans.Lattice.Views;

/// <summary>
/// Admission fence for new joint-repair obligations during a monotonic cluster
/// upgrade. Existing legacy operations keep their original completion contract;
/// closing this gate never discharges an already admitted repair obligation.
/// </summary>
internal interface IViewJointRepairAdmissionGate
{
    /// <summary>
    /// Whether every non-dead member advertises joint-repair support and at
    /// least one member is active. Unknown or unfetched manifests fail closed.
    /// </summary>
    Task<bool> IsOpenAsync(CancellationToken cancellationToken = default);
}
