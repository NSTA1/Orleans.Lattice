using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The run-wide hosts: the test world, the hostile-app head and the
/// re-authentication head, each started on first use and shared by every fixture,
/// and all stopped once, after the last fixture.
/// </summary>
/// <remarks>
/// Nothing starts until a test asks for it, so the hygiene fixtures in this assembly
/// run without a silo or a browser, and a shard that never touches the hostile head
/// never starts one.
/// </remarks>
[SetUpFixture]
public sealed class UiHosts
{
    private static readonly Lazy<Task<ExplorerWorld>> LazyWorld = new(ExplorerWorld.StartAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<Task<HostileAppHead>> LazyHostile = new(async () => await HostileAppHead.StartAsync(await LazyWorld.Value), LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly RenewableSignIn Renewable = new();
    private static readonly Lazy<Task<ExplorerHead>> LazyReauth = new(async () => await Renewable.StartHeadAsync(await LazyWorld.Value), LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<Task<ExplorerWorld>> LazyTenantWorld = new(ExplorerWorld.StartWithTenancyAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<Task<ExplorerWorld>> LazyDelegatedWorld = new(ExplorerWorld.StartWithDelegatedAccessAsync, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>The test world: a cluster, its gRPC surface and its own Explorer head.</summary>
    internal static Task<ExplorerWorld> WorldAsync() => LazyWorld.Value;

    /// <summary>A second world that also serves tenancy, with several tenants its operator can switch between.</summary>
    internal static Task<ExplorerWorld> TenantWorldAsync() => LazyTenantWorld.Value;

    /// <summary>A third world that serves tenancy with delegated tenant access administration switched on.</summary>
    internal static Task<ExplorerWorld> DelegatedWorldAsync() => LazyDelegatedWorld.Value;

    /// <summary>The head that offers the hostile app bundles.</summary>
    internal static Task<HostileAppHead> HostileAsync() => LazyHostile.Value;

    /// <summary>The head that offers a renewable token sign-in, and the switch that refuses its renewals.</summary>
    internal static async Task<(ExplorerHead Head, RenewableSignIn SignIn)> ReauthAsync() => (await LazyReauth.Value, Renewable);

    /// <summary>Web-first assertions wait long enough for a circuit on a loaded agent.</summary>
    [OneTimeSetUp]
    public void Configure() => Assertions.SetDefaultExpectTimeout(20_000);

    /// <summary>Stops every browser and host that was started.</summary>
    [OneTimeTearDown]
    public async Task StopAsync()
    {
        await UiBrowsers.DisposeAsync();

        if (LazyHostile.IsValueCreated && LazyHostile.Value.IsCompletedSuccessfully)
        {
            await LazyHostile.Value.Result.DisposeAsync();
        }

        if (LazyReauth.IsValueCreated && LazyReauth.Value.IsCompletedSuccessfully)
        {
            await LazyReauth.Value.Result.DisposeAsync();
        }

        if (LazyWorld.IsValueCreated && LazyWorld.Value.IsCompletedSuccessfully)
        {
            await LazyWorld.Value.Result.DisposeAsync();
        }

        if (LazyTenantWorld.IsValueCreated && LazyTenantWorld.Value.IsCompletedSuccessfully)
        {
            await LazyTenantWorld.Value.Result.DisposeAsync();
        }

        if (LazyDelegatedWorld.IsValueCreated && LazyDelegatedWorld.Value.IsCompletedSuccessfully)
        {
            await LazyDelegatedWorld.Value.Result.DisposeAsync();
        }

        // Last: every head serves from the one published content root.
        ExplorerPublishedAssets.Cleanup();
    }
}
