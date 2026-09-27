namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Default transport policy that denies every app-control call.</summary>
public sealed class DenyAppsApiAuthorizer : ILatticeAppsApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}
