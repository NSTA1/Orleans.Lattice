using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// A real in-process web head, fed the standalone head's static web asset runtime
/// manifest exactly as <c>dotnet run</c> feeds it, serves every Explorer UI and
/// AppKit asset with its full bytes - through the static asset path and through
/// the app frame's bootstrap route (issue #3831).
/// </summary>
/// <remarks>
/// This is the check a build-time assertion cannot make. The design tokens and
/// fonts were once linked into the UI from the documentation site: the build
/// manifest listed them with their full length, and the runtime then served each
/// as an empty 200, so the console rendered completely unstyled.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class ExplorerStaticAssetServingTests
{
    private const string UiContent = "_content/Orleans.Lattice.Explorer.UI";
    private const string AppKitContent = "_content/Orleans.Lattice.Explorer.AppKit";

    [Test]
    public async Task Every_ui_and_appkit_asset_is_served_with_its_full_bytes()
    {
        var assets = RuntimeAssets()
            .Where(asset => asset.Path.StartsWith(UiContent + "/", StringComparison.Ordinal)
                || asset.Path.StartsWith(AppKitContent + "/", StringComparison.Ordinal))
            .ToArray();
        await using var app = await CreateHostAsync();
        using var client = app.GetTestServer().CreateClient();

        var offenders = new List<string>();
        foreach (var asset in assets)
        {
            var response = await client.GetAsync("/" + asset.Path);
            var body = await response.Content.ReadAsByteArrayAsync();
            if (response.StatusCode != HttpStatusCode.OK || body.Length == 0 || !body.AsSpan().SequenceEqual(File.ReadAllBytes(asset.File)))
            {
                offenders.Add($"{asset.Path}: {(int)response.StatusCode}, {body.Length} bytes");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(assets.Select(asset => asset.Path), Does.Contain(UiContent + "/design/tokens.css"));
            Assert.That(assets.Select(asset => asset.Path), Does.Contain(UiContent + "/design/fonts/recursive-sans-linear.woff2"));
            Assert.That(assets.Select(asset => asset.Path), Does.Contain(AppKitContent + "/appkit/v1/tokens.css"));
            Assert.That(offenders, Is.Empty, string.Join(Environment.NewLine, offenders));
        });
    }

    [TestCase("frame.html")]
    [TestCase("boot.js")]
    [TestCase("lattice-app.css")]
    [TestCase("tokens.css")]
    [TestCase("fonts/recursive-sans-linear.woff2")]
    [TestCase("fonts/cascadia-mono.woff2")]
    public async Task The_frame_bootstrap_route_serves_the_kit_with_its_full_bytes(string file)
    {
        await using var app = await CreateHostAsync();
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/_apps/frame/v1/" + file);
        var body = await response.Content.ReadAsByteArrayAsync();
        var expected = File.ReadAllBytes(Path.Combine(
            HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "AppKit", "wwwroot", "appkit", "v1",
            file.Replace('/', Path.DirectorySeparatorChar)));

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(body, Is.EqualTo(expected), file + " must be served in full");
        });
    }

    private static string RuntimeManifestPath() => Path.Combine(
        HygieneRepository.FindRepoRoot(),
        "src", "lattice.explorer", "Web", "bin", StaticWebAssetManifest.Configuration, "net10.0",
        "Orleans.Lattice.Explorer.WebHost.staticwebassets.runtime.json");

    /// <summary>Every file the runtime manifest serves, with the file the runtime reads it from.</summary>
    private static IReadOnlyList<(string Path, string File)> RuntimeAssets()
    {
        var manifestPath = RuntimeManifestPath();
        Assert.That(File.Exists(manifestPath), Is.True, "the standalone head must be built: " + manifestPath);

        using var document = JsonDocument.Parse(File.ReadAllText(manifestPath));
        var roots = document.RootElement.GetProperty("ContentRoots").EnumerateArray().Select(root => root.GetString()!).ToArray();
        var assets = new List<(string, string)>();
        Walk(document.RootElement.GetProperty("Root"), string.Empty, roots, assets);
        return assets;
    }

    private static void Walk(JsonElement node, string path, string[] roots, List<(string, string)> assets)
    {
        if (node.TryGetProperty("Asset", out var asset) && asset.ValueKind == JsonValueKind.Object)
        {
            var root = roots[asset.GetProperty("ContentRootIndex").GetInt32()];
            var subPath = asset.GetProperty("SubPath").GetString()!;
            if (!subPath.EndsWith(".gz", StringComparison.Ordinal) && !subPath.EndsWith(".br", StringComparison.Ordinal))
            {
                assets.Add((path, Path.Combine(root, subPath.Replace('/', Path.DirectorySeparatorChar))));
            }
        }

        if (node.TryGetProperty("Children", out var children) && children.ValueKind == JsonValueKind.Object)
        {
            foreach (var child in children.EnumerateObject())
            {
                Walk(child.Value, path.Length == 0 ? child.Name : path + "/" + child.Name, roots, assets);
            }
        }
    }

    private static async Task<WebApplication> CreateHostAsync()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
        builder.WebHost.UseTestServer();

        // The head's own runtime manifest, loaded exactly as `dotnet run` loads it,
        // so the web root file provider serves what a running head serves.
        builder.WebHost.UseSetting(WebHostDefaults.StaticWebAssetsKey, RuntimeManifestPath());
        builder.WebHost.UseStaticWebAssets();
        builder.Services.AddLatticeExplorerWeb();
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        var app = builder.Build();
        app.UseStaticFiles();
        app.MapLatticeExplorer();
        await app.StartAsync();
        return app;
    }
}
