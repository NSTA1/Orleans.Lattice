using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tenant write-admission layer. Extends the single user-origin
/// data-mutation seam (<see cref="ThrowIfUserOriginSystemDataTree"/>) with an
/// additional, opt-in admission check that consults the DI-registered
/// <see cref="ITenantAdmissionController"/> for the caller's active tenant.
/// </summary>
/// <remarks>
/// <para>
/// The core library registers only the no-op
/// <see cref="NullTenantAdmissionController"/>, whose
/// <see cref="ITenantAdmissionController.IsActive"/> is always <c>false</c>, so
/// a cluster with no tenancy add-on takes the synchronous zero-allocation fast
/// path (<see cref="ThrowIfWriteNotAdmittedAsync"/> returns a completed
/// <see cref="ValueTask"/> before reading the active tenant or awaiting the
/// controller) and the write path stays byte-for-byte identical to the
/// pre-tenancy behaviour.
/// </para>
/// <para>
/// Admission is layered <em>after</em> the reserved-namespace structural guards
/// and shares their system-origin bypass: a write authored inside a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope (as the
/// tenancy layer's composed <c>t/</c> routing and the first-party add-ons are)
/// is never subjected to admission, exactly as it is never subjected to the
/// reserved-namespace rejection.
/// </para>
/// <para>
/// Admission is also layered <em>after</em> access-gate authorization, and that
/// ordering is load-bearing rather than incidental. The active tenant admission
/// accounts against is a caller-asserted, client-supplied value: it travels in
/// an unreserved request-context key that
/// <c>LatticeCapabilityStrippingCallFilter</c> deliberately does not strip, so
/// every consumer is obliged to re-validate it. The gate is what performs that
/// validation (<c>TenantGateEnforcer</c> resolves the target tree's owning
/// tenant and rejects a subject asserting a tenant it holds no membership of),
/// so only once the gate has admitted the call is the ambient tenant a
/// <em>validated</em> assertion rather than an unverified claim. Running
/// admission first would let an unauthorized caller name any tenant and have a
/// stateful, quota-consuming, rate-limiting evaluation charged to that victim.
/// The backup path already observes the same authorize-then-account discipline
/// (<c>LatticeBackupRestoreService</c> authorizes before opening the tenant
/// restore scope).
/// </para>
/// </remarks>
internal sealed partial class LatticeGrain
{
    // Lazily-resolved, activation-cached admission controller. The default core
    // registration is the inactive NullTenantAdmissionController, so a
    // single-tenant host pays a single service lookup on the first write and
    // then takes the inactive fast path on every subsequent write. Resolved
    // through GetService (not GetRequiredService) so a host that never
    // registered the seam - or a unit test with a bare service provider -
    // resolves null and is treated as inactive (admit all), never faulting.
    private ITenantAdmissionController? _admissionController;
    private bool _admissionControllerResolved;

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
    /// Consults the per-tenant <see cref="ITenantAdmissionController"/> for the
    /// active tenant and refuses a non-admitted write with
    /// <see cref="LatticeTenantAccessDeniedException"/>. Called at every
    /// user-origin data-mutation site immediately <em>after</em> that site's
    /// access-gate enforcement call, never before it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Returns a synchronously-completed <see cref="ValueTask"/> (no active
    /// tenant read, no await, no allocation) whenever the controller is absent
    /// or inactive, or the write is system-origin, so the default off path is
    /// zero-cost. Only an active controller on a user-origin write takes the
    /// asynchronous <see cref="AdmitOrThrowAsync"/> slow path.
    /// </para>
    /// <para>
    /// The post-enforcement placement is a security invariant, not a stylistic
    /// choice: admission reads a client-supplied active tenant that only the
    /// access gate validates. See the type-level remarks.
    /// </para>
    /// </remarks>
    private ValueTask ThrowIfWriteNotAdmittedAsync(CancellationToken cancellationToken)
    {
        var controller = AdmissionController;
        if (controller is not { IsActive: true } || LatticeAccessGateContext.IsSystemOrigin)
            return default;
        return AdmitOrThrowAsync(controller, cancellationToken);
    }

    /// <remarks>
    /// Reads <see cref="LatticeActiveTenantContext.Current"/> directly. That is
    /// only sound because every caller invokes this after access-gate
    /// enforcement has already validated the caller's membership of the
    /// asserted tenant; do not hoist any call site above its
    /// <c>Enforce*Async</c> partner.
    /// </remarks>
    private async ValueTask AdmitOrThrowAsync(
        ITenantAdmissionController controller,
        CancellationToken cancellationToken)
    {
        var tenant = LatticeActiveTenantContext.Current ?? TenantId.Default;
        var admitted = await controller.IsAdmittedAsync(tenant, TreeId, cancellationToken);
        if (!admitted)
            throw new LatticeTenantAccessDeniedException(
                $"Tenant '{tenant}' is not admitted to write to tree '{TreeId}'.");
    }

    // Slow-path continuations for the two non-async public write entry points
    // (SetAsync and DeleteAsync) whose synchronous fast path manages the
    // enforcement ValueTask by hand. They run only when an active controller
    // returns an incomplete admission decision, which by construction is after
    // enforcement has already completed, so they must not re-enforce.
    private async Task SetAdmitThenWriteAsync(
        ValueTask admit, string key, byte[] value, CancellationToken cancellationToken)
    {
        await admit;
        await SetGatedTailAsync(key, value, cancellationToken);
    }

    private async Task<bool> DeleteAdmitThenDeleteAsync(
        ValueTask admit, string key, CancellationToken cancellationToken)
    {
        await admit;
        return LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => DeleteRegisteredAsync(key, ct), cancellationToken)
            : await DeleteRegisteredAsync(key, cancellationToken);
    }
}
