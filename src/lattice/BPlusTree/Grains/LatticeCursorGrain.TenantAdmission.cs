using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tenant read admission for the durable cursor grain's <b>snapshot</b>
/// pages.
/// </summary>
/// <remarks>
/// <para>
/// The live key / entry cursors page through the public
/// <see cref="ILattice.KeysAsync"/> / <see cref="ILattice.EntriesAsync"/>
/// surface, so the data-plane grain charges them at its own access-gate seam
/// and this partial must not charge them again. The snapshot cursor is the
/// exception: it reads snapshot leaf grains directly, so it never crosses that
/// seam, and without this it was an unmetered whole-keyspace read a tenant
/// could open as often as it liked - the same noisy-neighbour hole the read
/// charge exists to close, reachable through a different verb.
/// </para>
/// <para>
/// Charged per page rather than once per cursor, because a page is the unit of
/// work the caller actually drives and a cursor is unbounded in length.
/// </para>
/// </remarks>
internal sealed partial class LatticeCursorGrain
{
    private ITenantAdmissionController? _admissionController;
    private bool _admissionControllerResolved;

    /// <summary>
    /// The registered admission controller, resolved once per activation through
    /// <see cref="ServiceProviderServiceExtensions.GetService{T}"/> so a host that
    /// never registered the seam resolves <c>null</c> and is treated as inactive.
    /// The core default is the inactive <see cref="NullTenantAdmissionController"/>.
    /// </summary>
    private ITenantAdmissionController? AdmissionController
    {
        get
        {
            if (!_admissionControllerResolved)
            {
                _admissionController = services.GetService<ITenantAdmissionController>();
                _admissionControllerResolved = true;
            }

            return _admissionController;
        }
    }

    /// <summary>
    /// Applies the per-tenant read charge for one snapshot page, once
    /// <paramref name="resolving"/> - the fail-closed key-filter resolution - has
    /// completed. Keeps the synchronously-completed case allocation-free.
    /// </summary>
    private ValueTask<Func<string, bool>?> ChargeSnapshotReadAsync(ValueTask<Func<string, bool>?> resolving)
    {
        if (resolving.IsCompletedSuccessfully)
        {
            ThrowIfReadNotAdmitted();
            return resolving;
        }

        return AwaitThenChargeSnapshotReadAsync(resolving);
    }

    private async ValueTask<Func<string, bool>?> AwaitThenChargeSnapshotReadAsync(
        ValueTask<Func<string, bool>?> resolving)
    {
        var filter = await resolving;
        ThrowIfReadNotAdmitted();
        return filter;
    }

    /// <summary>
    /// Consults the admission controller for the active tenant and refuses a
    /// non-admitted read.
    /// </summary>
    /// <remarks>
    /// Skips on <see cref="LatticeAccessGateContext.IsGateBypassed"/> - a
    /// system-origin turn, or an authorised view-maintenance scope - so a turn the
    /// gate never adjudicated is never charged: the active tenant is a
    /// caller-asserted value that only the gate validates, and charging an
    /// unadjudicated turn would let a caller burn a named victim's budget. It does
    /// <em>not</em> skip on the default null gate, which is a permissive policy
    /// rather than an absent adjudication: the caller is still an ordinary tenant
    /// caller and its page must still be metered.
    /// </remarks>
    private void ThrowIfReadNotAdmitted()
    {
        var controller = AdmissionController;
        if (controller is not { IsActive: true } || LatticeAccessGateContext.IsGateBypassed)
        {
            return;
        }

        var treeId = state.State.TreeId;
        var tenant = LatticeActiveTenantContext.Current ?? TenantId.Default;
        if (!controller.IsReadAdmitted(tenant, treeId))
        {
            throw new LatticeTenantAccessDeniedException(
                $"Tenant '{tenant}' is not admitted to read from tree '{treeId}'.");
        }
    }
}
