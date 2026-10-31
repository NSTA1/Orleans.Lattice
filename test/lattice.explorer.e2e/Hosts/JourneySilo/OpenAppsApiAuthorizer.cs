using Orleans.Lattice.Api.Apps.Grpc;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// Opens the app-control bindings' coarse transport gate, as the silo opens every other
/// facade's: the real enforcement is the deny-by-default access gate the apps facades
/// apply to the authenticated subject afterwards. The apps package ships no allow-all
/// authorizer of its own, so the journey host supplies this one.
/// </summary>
internal sealed class OpenAppsApiAuthorizer : ILatticeAppsApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken) =>
        Task.FromResult(true);
}
