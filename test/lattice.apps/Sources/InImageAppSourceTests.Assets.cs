using System.Text;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

public partial class InImageAppSourceTests
{
    private const string ManifestResource = "Contoso.Notes.AppManifest.json";
    private static readonly byte[] Script = Encoding.UTF8.GetBytes("document.body.dataset.ready = '1';");

    private static (InImageAppSource Source, FakeAppAssembly Assembly, List<string> Reads) CreateAssetSource(
        IReadOnlyDictionary<string, Func<Stream>> assets,
        string? prefix = null,
        string? manifest = null)
    {
        var reads = new List<string>();
        var assembly = new FakeAppAssembly(name =>
        {
            reads.Add(name);
            if (name == ManifestResource)
                return new MemoryStream(Encoding.UTF8.GetBytes(manifest ?? SourceTestManifests.Minimal("demo-app", "2.0.1")));
            return assets.TryGetValue(name, out var open) ? open() : null;
        });
        var registration = new InImageAppRegistration(Slug, assembly, ManifestResource);
        if (prefix is not null)
            registration = registration with { AssetResourcePrefix = prefix };
        return (SourceTestManifests.Source(registration), assembly, reads);
    }

    private static Dictionary<string, Func<Stream>> Bundle(string resourceName, byte[] content) =>
        new() { [resourceName] = () => new MemoryStream(content) };

    [Test]
    public async Task OpenAssetAsync_reads_the_conventionally_named_resource_and_verifies_it()
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.js.app.js", Script));

        var result = await source.OpenAssetAsync(Slug, Version, "js/app.js", SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.Opened));
        Assert.That(result.Content.ToArray(), Is.EqualTo(Script));
        Assert.That(result.MediaType, Is.EqualTo("text/javascript"));
        Assert.That(reads, Is.EqualTo(new[] { ManifestResource, "Contoso.Notes.ui.js.app.js" }));
    }

    [Test]
    public async Task OpenAssetAsync_uses_an_explicit_prefix_verbatim()
    {
        var (source, _, _) = CreateAssetSource(Bundle("Bundle-app.js", Script), prefix: "Bundle-");

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256(Script));

        Assert.That(result.IsOpened, Is.True);
    }

    [Test]
    public async Task OpenAssetAsync_re_verifies_on_every_open()
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));
        var digest = SourceTestManifests.Sha256(Script);

        var first = await source.OpenAssetAsync(Slug, Version, "app.js", digest);
        var second = await source.OpenAssetAsync(Slug, Version, "app.js", digest);

        Assert.That(first.IsOpened && second.IsOpened, Is.True);
        Assert.That(reads.Count(r => r == "Contoso.Notes.ui.app.js"), Is.EqualTo(2));
    }

    [Test]
    public async Task OpenAssetAsync_a_digest_mismatch_returns_no_bytes()
    {
        var (source, _, _) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256([0]));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.DigestMismatch));
        Assert.That(result.Content.IsEmpty, Is.True);
        Assert.That(result.ActualSha256, Is.EqualTo(SourceTestManifests.Sha256(Script)));
    }

    [Test]
    public async Task OpenAssetAsync_a_malformed_expected_digest_is_a_mismatch_without_reading_the_asset()
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256(Script).ToUpperInvariant());

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.DigestMismatch));
        Assert.That(reads, Is.EqualTo(new[] { ManifestResource }));
    }

    [TestCase("../app.js")]
    [TestCase("js/../../app.js")]
    [TestCase("/app.js")]
    [TestCase("js\\app.js")]
    [TestCase("JS/app.js")]
    [TestCase("app.js?x=1")]
    [TestCase("app.js#x")]
    [TestCase("./app.js")]
    [TestCase("js//app.js")]
    [TestCase("")]
    [TestCase("c:/app.js")]
    public async Task OpenAssetAsync_rejects_traversal_and_non_normalised_paths_without_any_read(string path)
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));

        var result = await source.OpenAssetAsync(Slug, Version, path, SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That(reads, Is.Empty);
    }

    [Test]
    public async Task OpenAssetAsync_refuses_a_media_type_a_bundle_cannot_declare()
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.tool.exe", Script));

        var result = await source.OpenAssetAsync(Slug, Version, "tool.exe", SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That(reads, Is.Empty);
    }

    [Test]
    public async Task OpenAssetAsync_an_unknown_slug_wrong_version_or_missing_resource_is_not_found()
    {
        var (source, _, _) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));
        var digest = SourceTestManifests.Sha256(Script);

        Assert.That((await source.OpenAssetAsync(AppSlug.Parse("other"), Version, "app.js", digest)).Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That((await source.OpenAssetAsync(Slug, AppVersion.Parse("9.0.0"), "app.js", digest)).Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That((await source.OpenAssetAsync(Slug, Version, "missing.js", digest)).Status, Is.EqualTo(AppAssetStatus.NotFound));
    }

    [Test]
    public async Task OpenAssetAsync_an_app_that_does_not_resolve_is_not_available()
    {
        var (source, _, reads) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script), manifest: "{ not json");

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(reads, Is.EqualTo(new[] { ManifestResource }));
    }

    [Test]
    public async Task OpenAssetAsync_a_duplicate_registration_is_not_available()
    {
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("demo-app", "2.0.1"));
        var source = SourceTestManifests.Source(
            SourceTestManifests.Registration("demo-app", assembly),
            SourceTestManifests.Registration("demo-app", assembly));

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
    }

    [Test]
    public async Task OpenAssetAsync_bounds_the_asset_size_for_seekable_and_unseekable_streams()
    {
        var atLimit = new byte[InImageAppSource.MaxAssetBytes];
        var overLimit = new byte[InImageAppSource.MaxAssetBytes + 1];
        var (source, _, _) = CreateAssetSource(new Dictionary<string, Func<Stream>>
        {
            ["Contoso.Notes.ui.big.json"] = () => new MemoryStream(overLimit),
            ["Contoso.Notes.ui.big-stream.json"] = () => new NonSeekableStream(overLimit),
            ["Contoso.Notes.ui.max-stream.json"] = () => new NonSeekableStream(atLimit),
        });

        var seekable = await source.OpenAssetAsync(Slug, Version, "big.json", SourceTestManifests.Sha256(overLimit));
        var unseekable = await source.OpenAssetAsync(Slug, Version, "big-stream.json", SourceTestManifests.Sha256(overLimit));
        var bounded = await source.OpenAssetAsync(Slug, Version, "max-stream.json", SourceTestManifests.Sha256(atLimit));

        Assert.That(seekable.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(unseekable.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(bounded.Status, Is.EqualTo(AppAssetStatus.Opened));
        Assert.That(bounded.Content.Length, Is.EqualTo(InImageAppSource.MaxAssetBytes));
        Assert.That(InImageAppSource.MaxAssetBytes, Is.EqualTo(2 * 1024 * 1024));
    }

    [Test]
    public async Task OpenAssetAsync_a_throwing_resource_provider_is_not_available()
    {
        var (source, _, _) = CreateAssetSource(new Dictionary<string, Func<Stream>>
        {
            ["Contoso.Notes.ui.app.js"] = () => throw new IOException("disk gone"),
        });

        var result = await source.OpenAssetAsync(Slug, Version, "app.js", SourceTestManifests.Sha256(Script));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(result.Errors.Single().Message, Does.Not.Contain("disk gone"));
    }

    [Test]
    public void OpenAssetAsync_rejects_null_path_or_digest()
    {
        var (source, _, _) = CreateAssetSource(Bundle("Contoso.Notes.ui.app.js", Script));

        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.OpenAssetAsync(Slug, Version, null!, "x"));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.OpenAssetAsync(Slug, Version, "app.js", null!));
    }
}
