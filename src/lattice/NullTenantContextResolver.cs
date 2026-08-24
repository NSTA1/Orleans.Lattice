namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ITenantContextResolver"/>: always resolves to the
/// reserved <see cref="TenantId.Default"/> tenant. Registered by
/// <c>AddLattice</c> as the safe default so a consumer of the seam always
/// resolves an instance even when the tenancy add-on is not registered,
/// preserving core's byte-for-byte behaviour. The tenancy package replaces it
/// with the real context-reading implementation.
/// </summary>
/// <remarks>
/// The default tenant and its wrapping <see cref="ValueTask{TResult}"/> are
/// cached in a <c>static readonly</c> field, so every call returns the same
/// synchronously-completed result with no per-call allocation.
/// </remarks>
internal sealed class NullTenantContextResolver : ITenantContextResolver
{
    private static readonly ValueTask<TenantId> DefaultResult = new(TenantId.Default);

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
        DefaultResult;

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        tenant = TenantId.Default;
        return true;
    }
}
