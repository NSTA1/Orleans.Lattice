using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Registers the observable gauges that report the <b>state</b> of the per-silo
/// <see cref="LeafResidentWorkingSet"/> (issue #2788): its resolved budget, the
/// bytes currently resident against that budget, and the number of registrations
/// held.
/// <para>
/// <b>Why gauges, when the working set already has a counter.</b>
/// <see cref="LatticeMetrics.LeafResidencySheds"/> reads zero both when the
/// ledger is empty because registration never happens and when the ledger is
/// populated and correctly under budget. Those have opposite remedies, and no
/// amount of priming separates them: priming makes a counter's <i>silence</i>
/// readable, but a counter observes an <b>action</b> and this question is about
/// <b>state</b>. A counter can report that something happened; it structurally
/// cannot report that nothing needed to. That is what these three gauges are
/// for, and it is why the fix is a different instrument kind rather than a
/// better-primed counter.
/// </para>
/// <para>
/// <b>Why registration is eager, from <c>AddLattice</c>.</b> The neighbouring
/// gauge sinks (<see cref="LatticeAdmissionMetrics"/>,
/// <see cref="LatticeStorageUsageMetrics"/>) register when their DI singleton is
/// first constructed, which is lazy. Deferring here would make the series appear
/// only once a leaf had already registered, so a scrape taken before the first
/// activation would show <i>no series at all</i> - reintroducing exactly the
/// absent-versus-zero ambiguity in the half whose entire purpose is to remove
/// it. Registering at silo build means a present-and-zero reading is a measured
/// zero and an absent series means the build does not carry the gauges.
/// </para>
/// <para>
/// <b>Why it observes <see cref="LeafResidentWorkingSet.Shared"/>.</b> The leaf
/// activation path resolves its working set from
/// <c>ActivationServices.GetService&lt;LeafResidentWorkingSet&gt;()</c> and falls
/// back to <see cref="LeafResidentWorkingSet.Shared"/>. Nothing in the library
/// registers that service, so the container never supplies one and production
/// always uses <see cref="LeafResidentWorkingSet.Shared"/>; the service lookup
/// exists so a test can inject a small deterministic budget. That invariant is
/// what makes observing the shared instance correct, so it is pinned by a guard
/// test rather than left as an assumption - were a host to start registering the
/// service, these gauges would silently describe an object nobody uses, which is
/// the same class of wrong-zero they exist to eliminate.
/// </para>
/// <para>
/// The gauges carry no tags. The working set is per-silo and spans every tree,
/// so a <c>tree</c> tag would be a category error; the silo dimension arrives
/// from the exporter's resource attributes.
/// </para>
/// </summary>
internal static class LeafResidencyMetrics
{
    private static readonly object RegistrationLock = new();
    private static bool _registered;

    /// <summary>
    /// Registers the resident-working-set gauges on
    /// <see cref="LatticeMetrics.Meter"/>. Process-wide and idempotent, so it is
    /// safe to call from every <c>AddLattice</c> in a multi-silo process;
    /// instruments cannot be unregistered, and registering the same name twice
    /// would publish a duplicate series.
    /// </summary>
    internal static void EnsureRegistered()
    {
        lock (RegistrationLock)
        {
            if (_registered)
            {
                return;
            }

            var meter = LatticeMetrics.Meter;

            meter.CreateObservableGauge(
                LatticeMetrics.LeafResidencyBudgetBytesName,
                static () => MeasureBudgetBytes(LeafResidentWorkingSet.Shared),
                unit: "By",
                description: "Resolved byte budget the per-silo resident leaf working set enforces. Read against the container's memory grant: a budget of the same order as the whole grant means the bound cannot engage and a zero shed count says nothing.");

            meter.CreateObservableGauge(
                LatticeMetrics.LeafResidencyResidentBytesName,
                static () => MeasureResidentBytes(LeafResidentWorkingSet.Shared),
                unit: "By",
                description: "Bytes currently accounted to live, un-shed leaf registrations in the per-silo resident leaf working set.");

            meter.CreateObservableGauge(
                LatticeMetrics.LeafResidencyRegistrationsName,
                static () => MeasureRegistrations(LeafResidentWorkingSet.Shared),
                unit: "{registration}",
                description: "Leaf registrations currently held by the per-silo resident leaf working set. Distinguishes an empty ledger from a populated one that is correctly under budget, which the shed counter reads as zero for both.");

            _registered = true;
        }
    }

    /// <summary>
    /// Projects the resolved budget of <paramref name="workingSet"/>. Separated
    /// from the gauge callback so the projection is exercised against an
    /// explicit, deterministic instance rather than only against the process
    /// singleton, whose budget is environmental.
    /// </summary>
    internal static Measurement<long> MeasureBudgetBytes(LeafResidentWorkingSet workingSet)
    {
        ArgumentNullException.ThrowIfNull(workingSet);
        return new Measurement<long>(workingSet.BudgetBytes);
    }

    /// <summary>Projects the resident bytes of <paramref name="workingSet"/>.</summary>
    internal static Measurement<long> MeasureResidentBytes(LeafResidentWorkingSet workingSet)
    {
        ArgumentNullException.ThrowIfNull(workingSet);
        return new Measurement<long>(workingSet.ResidentBytes);
    }

    /// <summary>Projects the registration count of <paramref name="workingSet"/>.</summary>
    internal static Measurement<long> MeasureRegistrations(LeafResidentWorkingSet workingSet)
    {
        ArgumentNullException.ThrowIfNull(workingSet);
        return new Measurement<long>(workingSet.RegisteredCount);
    }
}
