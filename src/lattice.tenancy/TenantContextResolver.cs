namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantContextResolver"/>: resolves the caller's active
/// tenant from the ambient <see cref="LatticeActiveTenantContext"/> and
/// re-validates it against the caller's own membership before it is allowed to
/// scope a tree name. Replaces the core
/// <c>NullTenantContextResolver</c> when the tenancy add-on is registered, so
/// <see cref="LatticeTenantExtensions.GetLatticeAsync(IServiceProvider, string, CancellationToken)"/>
/// composes an unqualified, tenant-local name into the active tenant's
/// <c>t/{tenant}/{name}</c> namespace instead of returning it unchanged.
/// </summary>
/// <remarks>
/// <para>
/// The active tenant is a caller-supplied <em>assertion</em> (it arrives on the
/// <c>lattice-active-tenant</c> header), never a fact, so it is re-validated
/// here through <see cref="ITenantPolicyEngine.ValidateActiveTenant"/> exactly as
/// <see cref="TenantGateEnforcer"/> re-validates it on the enforcement path. A
/// subject that may not act as the asserted tenant resolves the uninitialised
/// "no tenant" value, which
/// <c>LatticeTenantResolution.ComposeEffectiveTreeId</c> turns into a
/// <see cref="LatticeTenantAccessDeniedException"/> - the fail-closed contract
/// the seam documents. Validating here as well as at the gate is deliberate
/// defence in depth: this seam decides which grain is addressed, so leaving it
/// unvalidated would let an unauthorized assertion select another tenant's
/// namespace and rely solely on the gate to catch it.
/// </para>
/// <para>
/// A request that asserts <em>no</em> active tenant resolves
/// <see cref="TenantId.Default"/> and is returned unchanged, matching the
/// compatibility carve-out <see cref="TenantGateEnforcer"/> applies to a bare
/// legacy tree: tenant adoption is non-destructive, so an opted-in cluster's
/// existing tenant-unaware clients keep addressing their bare tree ids.
/// </para>
/// <para>
/// The warm path is allocation- and await-free. It resolves the subject through
/// <see cref="ILatticeMembershipContext.TryResolveCurrent"/> (a warm cache read,
/// no directory I/O) and only falls back to the asynchronous resolution on a
/// cache miss. That slow path runs inside
/// <see cref="LatticeSystemOrigin.Enter"/> because membership resolution reads
/// gated directory trees, exactly as the tenant-admin authorizer does.
/// </para>
/// </remarks>
internal sealed class TenantContextResolver(
    ITenantPolicyEngine engine,
    ILatticeMembershipContext membership) : ITenantContextResolver
{
    private static readonly ValueTask<TenantId> DefaultResult = new(TenantId.Default);

    private readonly ITenantPolicyEngine _engine =
        engine ?? throw new ArgumentNullException(nameof(engine));

    private readonly ILatticeMembershipContext _membership =
        membership ?? throw new ArgumentNullException(nameof(membership));

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        // No assertion: default-tenant adoption. Resolved synchronously with no
        // membership read at all, so a tenant-unaware client pays nothing.
        if (LatticeActiveTenantContext.Current is not { Value: not null } asserted)
        {
            tenant = TenantId.Default;
            return true;
        }

        // An assertion must be validated against the caller's membership, which
        // needs the subject. Fall to the async path when it is not warm.
        if (!_membership.TryResolveCurrent(out var subject))
        {
            tenant = default;
            return false;
        }

        tenant = Validate(subject, asserted);
        return true;
    }

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (LatticeActiveTenantContext.Current is not { Value: not null } asserted)
        {
            return DefaultResult;
        }

        if (_membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<TenantId>(Validate(subject, asserted));
        }

        return ResolveSlowAsync(asserted, cancellationToken);
    }

    private async ValueTask<TenantId> ResolveSlowAsync(TenantId asserted, CancellationToken cancellationToken)
    {
        // Membership resolution reads the gated directory trees, so the miss path
        // runs system-origin; the warm path above never needs it.
        using (LatticeSystemOrigin.Enter())
        {
            var subject = await _membership.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
            return Validate(subject, asserted);
        }
    }

    /// <summary>
    /// Returns <paramref name="asserted"/> when <paramref name="subject"/> may act
    /// as it, and the uninitialised "no tenant" value (a fail-closed denial)
    /// otherwise. An anonymous subject can never act as a tenant.
    /// </summary>
    private TenantId Validate(LatticeSubject subject, TenantId asserted)
    {
        if (subject.IsAnonymous)
        {
            return default;
        }

        return _engine.ValidateActiveTenant(subject.SubjectId, asserted).Allowed
            ? asserted
            : default;
    }
}
