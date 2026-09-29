using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text.Json;
using Orleans.Lattice.Api.Apps;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One hostile app bundle read from its fixture folder, following the harness
/// contract in X1's <c>Fixtures/HostileBundles/README.md</c>: the app is named by
/// <c>displayName</c>, declares <c>trees</c>, and its UI's entry, styles, scripts and
/// bridge grants come from <c>fixture.json</c>. Every other file is an asset whose
/// digest is computed from the bytes on disk, and a <c>serve</c> substitution serves
/// another file's bytes under the pinned path and digest, which is how a compromised
/// source is simulated.
/// </summary>
internal sealed class HostileBundle
{
    private HostileBundle(string name, JsonElement expect, WorkspaceAppDescriptor descriptor, ImmutableDictionary<string, AppUiAsset> served)
    {
        Name = name;
        Expect = expect;
        Descriptor = descriptor;
        Served = served;
    }

    /// <summary>The fixture's name, which is also the app's slug.</summary>
    public string Name { get; }

    /// <summary>The fixture's <c>expect</c> object.</summary>
    public JsonElement Expect { get; }

    /// <summary>What the caller's workspace describes: an enabled install with this UI.</summary>
    public WorkspaceAppDescriptor Descriptor { get; }

    /// <summary>The bytes the workspace serves for each path, after any substitution.</summary>
    public ImmutableDictionary<string, AppUiAsset> Served { get; }

    /// <summary>The app's display name, which prefixes every toast the host shows for it.</summary>
    public string DisplayName => Descriptor.Presentation!.DisplayName;

    /// <summary>The failure the host must show, or <see langword="null"/> when the frame must keep running.</summary>
    public string? ExpectedFailure =>
        Expect.TryGetProperty("failure", out var failure) && failure.ValueKind == JsonValueKind.String ? failure.GetString() : null;

    /// <summary>The exact toast the host must show, or <see langword="null"/>.</summary>
    public string? ExpectedToast =>
        Expect.TryGetProperty("toast", out var toast) && toast.ValueKind == JsonValueKind.String ? toast.GetString() : null;

    /// <summary>Reads the fixture in <paramref name="folder"/>.</summary>
    /// <param name="folder">The fixture folder.</param>
    public static HostileBundle Load(string folder)
    {
        var fixture = JsonDocument.Parse(File.ReadAllText(Path.Combine(folder, "fixture.json"))).RootElement.Clone();
        var name = fixture.GetProperty("name").GetString()!;

        var files = Directory.GetFiles(folder)
            .Select(Path.GetFileName)
            .OfType<string>()
            .Where(file => file != "fixture.json")
            .Order(StringComparer.Ordinal)
            .ToArray();

        var bytes = files.ToDictionary(file => file, file => File.ReadAllBytes(Path.Combine(folder, file)), StringComparer.Ordinal);
        var descriptors = files
            .Select(file => new AppUiAssetDescriptor { Path = file, MediaType = MediaType(file), Sha256 = Sha(bytes[file]) })
            .ToImmutableArray();

        var substitutions = fixture.TryGetProperty("serve", out var serve)
            ? serve.EnumerateObject().ToDictionary(pair => pair.Name, pair => pair.Value.GetString()!, StringComparer.Ordinal)
            : new Dictionary<string, string>(StringComparer.Ordinal);

        var served = descriptors.ToImmutableDictionary(
            asset => asset.Path,
            asset => new AppUiAsset
            {
                Path = asset.Path,
                MediaType = asset.MediaType,
                Sha256 = asset.Sha256,
                Bytes = bytes[substitutions.GetValueOrDefault(asset.Path, asset.Path)],
            },
            StringComparer.Ordinal);

        var ui = new AppUiDescriptor
        {
            Entry = fixture.GetProperty("entry").GetString()!,
            Styles = [.. fixture.GetProperty("styles").EnumerateArray().Select(style => style.GetString()!)],
            Scripts = [.. fixture.GetProperty("scripts").EnumerateArray().Select(script => new AppUiScriptDescriptor
            {
                Path = script.GetProperty("path").GetString()!,
                Module = script.GetProperty("module").GetBoolean(),
            })],
            Assets = descriptors,
            BundleDigest = F1.AppUiBundle.ComputeBundleDigest(descriptors
                .Select(asset => new F1.AppUiAsset { Path = asset.Path, MediaType = asset.MediaType, Digest = asset.Sha256 })
                .ToArray()),
            Bridge = [.. fixture.GetProperty("bridge").EnumerateArray().Select(grant => new AppUiBridgeGrantDescriptor
            {
                Operation = grant.GetProperty("operation").GetString()!,
                Tree = grant.TryGetProperty("tree", out var tree) ? tree.GetString() : null,
            })],
            MinProtocol = 1,
        };

        var descriptor = new WorkspaceAppDescriptor
        {
            Slug = name,
            Version = "1.0.0",
            InstallRevision = 1,
            SourceKey = "hostile-fixtures",
            State = AppLifecycleState.Enabled,
            Presentation = new AppPresentationDescriptor { DisplayName = fixture.GetProperty("displayName").GetString()! },
            Trees = [.. fixture.GetProperty("trees").EnumerateArray().Select(tree => new WorkspaceTreeDescriptor { Name = tree.GetString()! })],
            Ui = ui,
        };

        return new HostileBundle(name, fixture.GetProperty("expect").Clone(), descriptor, served);
    }

    private static string Sha(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    private static string MediaType(string file) => Path.GetExtension(file) switch
    {
        ".html" => "text/html",
        ".css" => "text/css",
        ".js" or ".mjs" => "text/javascript",
        ".svg" => "image/svg+xml",
        ".png" => "image/png",
        ".json" => "application/json",
        _ => throw new InvalidOperationException($"The hostile fixture file '{file}' has no media type the harness knows."),
    };
}
