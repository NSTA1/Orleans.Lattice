using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// An Explorer head whose app workspace and app bridge are the hostile-bundle
/// harness's own, following X1's harness contract: every hostile fixture is an
/// installed, enabled app the signed-in user may open, its assets come from the
/// fixture folder (with any <c>serve</c> substitution), and the bridge is a recorder
/// that stands in for the cluster, so a test can prove a request never reached it.
/// </summary>
/// <remarks>
/// <para>
/// Everything else - sign-in, every other area, the frame host, the broker - is the
/// real Explorer talking to the test world's cluster, which is also where the user
/// signs in. The two facades are registered under the Explorer's facade key before
/// the head registers its own, so they win its <c>TryAdd</c>; an unkeyed registration
/// would be ignored and the real transport would answer instead.
/// </para>
/// </remarks>
internal sealed class HostileAppHead : IAsyncDisposable
{
    /// <summary>The head-relative path the <c>navigate-out</c> bundle sends its frame to.</summary>
    public const string EscapePath = "/uitest/escape";

    /// <summary>The head-relative folder the AppKit boot fixture page is served from.</summary>
    public const string BootFixturePath = "/uitest/appkit/";

    private HostileAppHead(ExplorerHead head, IReadOnlyDictionary<string, HostileBundle> bundles, RecordingBridge bridge, SemaphoreSlim escapes)
    {
        Head = head;
        Bundles = bundles;
        Bridge = bridge;
        Escapes = escapes;
    }

    /// <summary>The Explorer head.</summary>
    public ExplorerHead Head { get; }

    /// <summary>Every hostile bundle the workspace offers, by slug.</summary>
    public IReadOnlyDictionary<string, HostileBundle> Bundles { get; }

    /// <summary>The recorder standing in for the cluster behind the bridge.</summary>
    public RecordingBridge Bridge { get; }

    /// <summary>
    /// Released once for every request that reached <see cref="EscapePath"/>: the proof that a
    /// frame's attempt to navigate out was made, not merely that nothing was seen.
    /// </summary>
    public SemaphoreSlim Escapes { get; }

    /// <summary>
    /// Starts the head on the world's cluster, offering X1's hostile fixtures and this
    /// suite's own.
    /// </summary>
    /// <param name="world">The world the head signs in against.</param>
    public static async Task<HostileAppHead> StartAsync(ExplorerWorld world)
    {
        ArgumentNullException.ThrowIfNull(world);

        var bundles = HostileBundleFolders()
            .Select(HostileBundle.Load)
            .ToDictionary(bundle => bundle.Name, StringComparer.Ordinal);
        var workspace = new HostileWorkspace(bundles);
        var bridge = new RecordingBridge();
        var escapes = new SemaphoreSlim(0);
        var key = ExplorerFacadeKey.Value;
        var bootFixtures = RepositoryPaths.Resolve("test/lattice.explorer/AppKit/Fixtures");

        var head = await ExplorerHead.StartAsync(new ExplorerHeadOptions
        {
            Endpoint = world.GrpcEndpoint,
            ConfigureServices = services =>
            {
                services.AddKeyedSingleton<ILatticeAppWorkspace>(key, workspace);
                services.AddKeyedSingleton<ILatticeAppBridge>(key, bridge);
            },
            ConfigureApp = app =>
            {
                // A page of the Explorer's own origin outside the frame bootstrap. It is
                // served with the Explorer's framing policy like every other page, so a
                // browser must refuse to render it in the app's frame; if it ever did, its
                // script would tell the Explorer page.
                app.MapGet(EscapePath, () =>
                {
                    escapes.Release();
                    return Results.Content(
                        "<!doctype html><title>escaped</title><script>parent.postMessage('escaped', '*'); top.postMessage('escaped', '*');</script>",
                        "text/html");
                });

                // F5's AppKit boot fixture, served from the Explorer's origin so it may frame
                // the real bootstrap document, which only a same-origin page may embed.
                app.MapGet(BootFixturePath + "{file}", (string file) => file switch
                {
                    "boot-fixture.html" => Results.File(Path.Combine(bootFixtures, file), "text/html"),
                    "boot-fixture.js" => Results.File(Path.Combine(bootFixtures, file), "text/javascript"),
                    _ => Results.NotFound(),
                });
            },
        });

        return new HostileAppHead(head, bundles, bridge, escapes);
    }

    /// <summary>The fixture folders: X1's hostile bundles, then this suite's own.</summary>
    public static IEnumerable<string> HostileBundleFolders() =>
        Directory.GetDirectories(RepositoryPaths.Resolve("test/lattice.explorer/UI/AppFrame/Fixtures/HostileBundles"))
            .Concat(Directory.GetDirectories(RepositoryPaths.Resolve("test/lattice.explorer.uitests/Apps/Bundles")))
            .Order(StringComparer.Ordinal);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => Head.DisposeAsync();

    /// <summary>The signed-in user's workspace: every hostile bundle, installed and enabled.</summary>
    private sealed class HostileWorkspace(IReadOnlyDictionary<string, HostileBundle> bundles) : ILatticeAppWorkspace
    {
        private readonly ImmutableArray<WorkspaceAppSummary> _apps = [.. bundles.Values
            .OrderBy(bundle => bundle.Name, StringComparer.Ordinal)
            .Select(bundle => new WorkspaceAppSummary
            {
                Slug = bundle.Name,
                Version = bundle.Descriptor.Version,
                InstallRevision = bundle.Descriptor.InstallRevision,
                Presentation = bundle.Descriptor.Presentation,
                HasUi = true,
                Roles = ["viewer"],
            })];

        public Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(_apps);

        public Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default) =>
            Task.FromResult(bundles.TryGetValue(appSlug, out var bundle) ? bundle.Descriptor : null);

        public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default) =>
            Task.FromResult<AppIconAsset?>(null);

        public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default) =>
            Task.FromResult(bundles.TryGetValue(appSlug, out var bundle) && bundle.Served.TryGetValue(path, out var asset) ? asset : null);
    }
}
