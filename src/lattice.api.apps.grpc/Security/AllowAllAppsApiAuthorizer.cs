namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Explicit opt-in for hosts whose endpoint is protected by an outer authentication boundary.</summary>
public sealed class AllowAllAppsApiAuthorizer : ILatticeAppsApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
