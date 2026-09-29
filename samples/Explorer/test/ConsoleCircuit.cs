using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// One console circuit opened directly on the console host's services, the way a
/// browser's Blazor circuit is: its own scope, so its own connection, sign-in,
/// tenant context and credential-aware facades. It reaches the cluster only
/// through the Explorer's own transport, so what it sees is what the console
/// shows that identity at that tenant's address.
/// </summary>
internal sealed class ConsoleCircuit : IAsyncDisposable
{
    /// <summary>
    /// The key the Explorer registers its facades under (the Explorer UI's
    /// <c>ShellFacades.Key</c>, which is internal to it).
    /// </summary>
    private const string FacadeKey = "Orleans.Lattice.Explorer.UI.Transport";

    private readonly AsyncServiceScope _scope;

    private ConsoleCircuit(AsyncServiceScope scope) => _scope = scope;

    private IServiceProvider Services => _scope.ServiceProvider;

    /// <summary>The Explorer's apps facade for this circuit.</summary>
    public ILatticeAppsControl Apps => Services.GetRequiredKeyedService<ILatticeAppsControl>(FacadeKey);

    /// <summary>Opens a circuit signed in as <paramref name="user"/> and scoped to <paramref name="tenant"/>.</summary>
    /// <param name="sample">The started sample.</param>
    /// <param name="user">The identity to sign in as.</param>
    /// <param name="tenant">The tenant the circuit is scoped to.</param>
    public static async Task<ConsoleCircuit> OpenAsync(ExplorerSample sample, string user, string tenant)
    {
        var circuit = new ConsoleCircuit(sample.ConsoleRegion.Services.CreateAsyncScope());
        try
        {
            await circuit.Services.GetRequiredService<IExplorerSession>().InitializeAsync();

            // The console keeps a sign-in in a cookie, which a browser's sign-in
            // request carries; this circuit signs in within a stand-in request.
            circuit.Services.GetRequiredService<IHttpContextAccessor>().HttpContext =
                new DefaultHttpContext { RequestServices = circuit.Services };
            await circuit.Services.GetRequiredService<IExplorerAuthSession>().LoginAsync(user, SampleIdentities.AdministratorPassword);
            circuit.ScopeTo(tenant);
            return circuit;
        }
        catch
        {
            await circuit.DisposeAsync();
            throw;
        }
    }

    /// <summary>Scopes the circuit to <paramref name="tenant"/>, as moving to its address does.</summary>
    /// <param name="tenant">The tenant.</param>
    public void ScopeTo(string tenant) =>
        Services.GetRequiredService<IExplorerTenantContext>().ActiveTenant = new ExplorerTenantId(tenant);

    /// <summary>The tree ids the circuit's state connection lists.</summary>
    public async Task<IReadOnlyList<string>> ListTreesAsync()
    {
        var connection = Services.GetRequiredService<IExplorerSession>().Connection;
        var trees = new List<string>();
        string? token = null;
        do
        {
            var page = await connection.ListTreesAsync(new CatalogRequest { PageSize = CatalogRequest.MaxPageSize, PageToken = token });
            trees.AddRange(page.Entries.Select(entry => entry.TreeId));
            token = page.NextPageToken;
        }
        while (token is not null);

        return trees;
    }

    /// <summary>The slugs of the apps installed in the circuit's tenant.</summary>
    public async Task<IReadOnlyList<string>> ListInstalledAppsAsync() =>
        [.. (await Apps.ListAsync()).Apps.Select(app => app.Slug)];

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _scope.DisposeAsync();
}
