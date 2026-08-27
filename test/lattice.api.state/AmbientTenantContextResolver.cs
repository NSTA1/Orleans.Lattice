namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Test double for <see cref="ITenantContextResolver"/> that resolves the tenant
/// stamped on the ambient <see cref="LatticeActiveTenantContext"/>, mirroring the
/// shape of the real tenancy add-on's resolver without pulling the add-on (and
/// its membership / policy dependencies) into this package's unit tests.
/// </summary>
/// <remarks>
/// <para>
/// The three constructor switches cover the three behaviours a facade must
/// survive: the synchronous warm path (the default, and what both the core no-op
/// resolver and the real resolver take when a tenant assertion is already
/// validated), the asynchronous fallback (a membership cache miss), and a
/// fail-closed denial (an assertion the caller may not make), which resolves the
/// uninitialised "no tenant" value the core turns into a
/// <see cref="LatticeTenantAccessDeniedException"/>.
/// </para>
/// <para>
/// No ambient tenant resolves <see cref="TenantId.Default"/>, which is exactly
/// what a tenancy-off cluster does, so the same double also pins the unchanged
/// bare-name behaviour.
/// </para>
/// </remarks>
internal sealed class AmbientTenantContextResolver(
    bool resolveSynchronously = true,
    bool deny = false) : ITenantContextResolver
{
    /// <summary>The number of times the synchronous fast path resolved a tenant.</summary>
    public int SynchronousResolutions { get; private set; }

    /// <summary>The number of times the asynchronous fallback resolved a tenant.</summary>
    public int AsynchronousResolutions { get; private set; }

    /// <summary>The total number of resolutions over either path.</summary>
    public int Resolutions => SynchronousResolutions + AsynchronousResolutions;

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        if (!resolveSynchronously)
        {
            tenant = default;
            return false;
        }

        SynchronousResolutions++;
        tenant = Resolve();
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        // Yield so the caller genuinely suspends, proving the facade's slow path
        // is correct rather than only ever exercising a completed ValueTask.
        await Task.Yield();
        AsynchronousResolutions++;
        return Resolve();
    }

    private TenantId Resolve()
    {
        if (deny)
        {
            // The fail-closed contract: an unattributable request resolves the
            // uninitialised "no tenant" value, never a silent default.
            return default;
        }

        return LatticeActiveTenantContext.Current is { Value: not null } tenant
            ? tenant
            : TenantId.Default;
    }
}
