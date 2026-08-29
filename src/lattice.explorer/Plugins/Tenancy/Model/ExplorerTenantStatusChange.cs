namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The outcome of a tenant suspend or resume: where the tenant was, where it is
/// now, and whether the call moved it. The transitions are idempotent, so
/// suspending an already-suspended tenant reports <see cref="Changed"/>
/// <see langword="false"/> rather than failing.
/// </summary>
/// <param name="TenantId">The tenant whose lifecycle state was addressed.</param>
/// <param name="PreviousStatus">The state the tenant was in before the call.</param>
/// <param name="NewStatus">The state the tenant is in after the call.</param>
/// <param name="Changed"><see langword="true"/> when the call moved the tenant.</param>
public readonly record struct ExplorerTenantStatusChange(
    string TenantId,
    ExplorerTenantLifecycle PreviousStatus,
    ExplorerTenantLifecycle NewStatus,
    bool Changed);
