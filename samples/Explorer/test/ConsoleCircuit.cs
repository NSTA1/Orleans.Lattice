using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TenantAdmin;
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

    /// <summary>
    /// The circuit's own services, as its Blazor components resolve them: a
    /// component rendered over them sees the cluster as this circuit does.
    /// </summary>
    public IServiceProvider Services => _scope.ServiceProvider;

    /// <summary>The Explorer's apps facade for this circuit.</summary>
    public ILatticeAppsControl Apps => Services.GetRequiredKeyedService<ILatticeAppsControl>(FacadeKey);

    /// <summary>The Explorer's cross-tenant grant facade for this circuit.</summary>
    public ILatticeTenantGrantAdmin Grants => Services.GetRequiredKeyedService<ILatticeTenantGrantAdmin>(FacadeKey);

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

    /// <summary>Reads <paramref name="key"/> from <paramref name="treeId"/> through the circuit's state connection, as the tree workspace does.</summary>
    /// <param name="treeId">The tree id, exactly as the workspace passes it.</param>
    /// <param name="key">The key.</param>
    public Task<EntryGetResponse> ReadAsync(string treeId, string key) =>
        Services.GetRequiredService<IExplorerSession>().Connection.GetEntryAsync(new EntryGetRequest { TreeId = treeId, Key = key });

    /// <summary>The slugs of the apps installed in the circuit's tenant.</summary>
    public async Task<IReadOnlyList<string>> ListInstalledAppsAsync() =>
        [.. (await Apps.ListAsync()).Apps.Select(app => app.Slug)];

    /// <summary>
    /// The tenants the console offers this circuit to switch between: the one list the
    /// address line's <c>t/</c> completions, the tenant directory and the top-bar tenant
    /// switcher all read.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListAccessibleTenantsAsync() =>
        [.. (await Services.GetRequiredService<IExplorerAccessibleTenantSource>().GetAccessibleTenantsAsync()).Select(tenant => tenant.Value)];

    /// <summary>Renders <typeparamref name="TComponent"/> in this circuit, as the console would, and returns its markup.</summary>
    /// <typeparam name="TComponent">A console component.</typeparam>
    public async Task<string> RenderAsync<TComponent>()
        where TComponent : IComponent
    {
        await using var renderer = new HtmlRenderer(Services, Services.GetRequiredService<ILoggerFactory>());
        return await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync<TComponent>()).ToHtmlString());
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _scope.DisposeAsync();
}
