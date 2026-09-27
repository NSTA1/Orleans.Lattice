namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Host-supplied admission policy for app-control RPCs; the default policy denies every call.</summary>
public interface ILatticeAppsApiAuthorizer
{
    /// <summary>Admits or denies an operation and its asserted app target. Facade authorization remains independent.</summary>
    Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}
