using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Orleans.Lattice.Api.Apps;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// A small, valid app UI install for the frame tests: three assets whose digests and bundle
/// digest are computed with F1's own <c>AppUiBundle</c>, so the host's port is exercised
/// against the authoritative algorithm rather than against itself.
/// </summary>
internal static class AppFrameTestData
{
    public const string Slug = "taskboard";
    public const string Version = "1.0.0";
    public const long Revision = 7;
    public const string DisplayName = "Task Board";
    public const string Entry = "index.html";
    public const string Style = "app.css";
    public const string Script = "app.js";

    public static readonly byte[] EntryBytes = Encoding.UTF8.GetBytes("<main id=\"app\">Hello</main>");
    public static readonly byte[] StyleBytes = Encoding.UTF8.GetBytes("main { margin: 0; }");
    public static readonly byte[] ScriptBytes = Encoding.UTF8.GetBytes("lattice.ready.then(() => {});");

    /// <summary>Every operation granted, the data ones over every declared tree.</summary>
    public static ImmutableArray<AppUiBridgeGrantDescriptor> AllGrants { get; } =
        F1.AppUiBridgeOperations.All.Select(op => new AppUiBridgeGrantDescriptor { Operation = op }).ToImmutableArray();

    /// <summary>The lower-case hexadecimal SHA-256 of <paramref name="bytes"/>.</summary>
    public static string Sha(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    /// <summary>The descriptor's asset list.</summary>
    public static ImmutableArray<AppUiAssetDescriptor> AssetDescriptors { get; } =
    [
        new() { Path = Entry, MediaType = "text/html", Sha256 = Sha(EntryBytes) },
        new() { Path = Style, MediaType = "text/css", Sha256 = Sha(StyleBytes) },
        new() { Path = Script, MediaType = "text/javascript", Sha256 = Sha(ScriptBytes) },
    ];

    /// <summary>F1's bundle digest of an asset list.</summary>
    public static string BundleDigest(IEnumerable<AppUiAssetDescriptor> assets) =>
        F1.AppUiBundle.ComputeBundleDigest(assets
            .Select(asset => new F1.AppUiAsset { Path = asset.Path, MediaType = asset.MediaType, Digest = asset.Sha256 })
            .ToArray());

    /// <summary>A valid UI declaration.</summary>
    public static AppUiDescriptor Ui(ImmutableArray<AppUiBridgeGrantDescriptor>? grants = null) => new()
    {
        Entry = Entry,
        Styles = [Style],
        Scripts = [new AppUiScriptDescriptor { Path = Script, Module = true }],
        Assets = AssetDescriptors,
        BundleDigest = BundleDigest(AssetDescriptors),
        Bridge = grants ?? AllGrants,
        MinProtocol = 1,
    };

    /// <summary>A valid, enabled description.</summary>
    public static WorkspaceAppDescriptor Describe(AppUiDescriptor? ui = null, long revision = Revision, string version = Version) => new()
    {
        Slug = Slug,
        Version = version,
        InstallRevision = revision,
        SourceKey = "in-image",
        State = AppLifecycleState.Enabled,
        Presentation = new AppPresentationDescriptor { DisplayName = DisplayName },
        Trees = [new WorkspaceTreeDescriptor { Name = "orders" }, new WorkspaceTreeDescriptor { Name = "notes" }],
        Ui = ui ?? Ui(),
    };

    /// <summary>The workspace's verified assets for the default bundle.</summary>
    public static IEnumerable<AppUiAsset> Assets() =>
    [
        Asset(Entry, "text/html", EntryBytes),
        Asset(Style, "text/css", StyleBytes),
        Asset(Script, "text/javascript", ScriptBytes),
    ];

    /// <summary>One workspace asset whose digest matches its bytes.</summary>
    public static AppUiAsset Asset(string path, string mediaType, byte[] bytes) => new()
    {
        Path = path,
        MediaType = mediaType,
        Sha256 = Sha(bytes),
        Bytes = bytes,
    };

    /// <summary>A workspace granting the default app.</summary>
    public static FakeAppWorkspace Workspace(WorkspaceAppDescriptor? descriptor = null) =>
        new FakeAppWorkspace().Grant(descriptor ?? Describe(), Assets());
}
