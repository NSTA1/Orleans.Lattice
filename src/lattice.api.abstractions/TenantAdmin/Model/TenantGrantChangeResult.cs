namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of a cross-tenant grant lifecycle operation (offer, approve,
/// reject, or revoke): the grant as it stands after the call, and whether the
/// call actually wrote to the registry. Every operation is idempotent, so
/// <see cref="Changed"/> reports <see langword="false"/> when the grant was
/// already in the requested state and nothing was written.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGrantChangeResult)]
[Immutable]
public sealed record TenantGrantChangeResult
{
    /// <summary>
    /// The grant as committed. This is the <em>converged</em> grant the registry's
    /// CRDT merge produced, so it also reflects a concurrent transition written by
    /// the other party rather than only this call's own change - which is why a
    /// caller must read <see cref="TenantGrantDescriptor.State"/> here rather than
    /// assume the state it asked for.
    /// </summary>
    [Id(0)] public required TenantGrantDescriptor Grant { get; init; }

    /// <summary><see langword="true"/> when the call wrote to the registry; <see langword="false"/> for an idempotent no-op.</summary>
    [Id(1)] public required bool Changed { get; init; }
}
