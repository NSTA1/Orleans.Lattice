using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>Unit tests for the facade's small internal helpers: version precedence, the listing continuation, bridge mapping and asset reads.</summary>
[TestFixture]
public sealed class AppsCatalogHelperTests
{
    [TestCase("1.0.0", "1.0.0", 0)]
    [TestCase("1.0.0", "2.0.0", -1)]
    [TestCase("1.10.0", "1.9.0", 1)]
    [TestCase("1.0.0-rc.1", "1.0.0", -1)]
    [TestCase("1.0.0-alpha", "1.0.0-alpha.1", -1)]
    [TestCase("1.0.0-alpha.1", "1.0.0-alpha.beta", -1)]
    [TestCase("1.0.0-beta.2", "1.0.0-beta.11", -1)]
    [TestCase("1.0.0-rc.1", "1.0.0-beta", 1)]
    [TestCase("1.0.0+build.1", "1.0.0+build.2", 0)]
    [TestCase("10.0.0", "9.99.99", 1)]
    public void AppVersionPrecedence_orders_by_semantic_version_precedence(string left, string right, int expected)
    {
        Assert.That(Math.Sign(AppVersionPrecedence.Compare(left, right)), Is.EqualTo(expected));
        Assert.That(Math.Sign(AppVersionPrecedence.Compare(right, left)), Is.EqualTo(-expected));
    }

    [Test]
    public void AppVersionPrecedence_rejects_null()
    {
        Assert.Throws<ArgumentNullException>(() => AppVersionPrecedence.Compare(null!, "1.0.0"));
        Assert.Throws<ArgumentNullException>(() => AppVersionPrecedence.Compare("1.0.0", null!));
    }

    [Test]
    public void AppCatalogContinuation_round_trips_every_cursor_state()
    {
        IAppCatalogSource[] sources = [new TestCatalogSource("alpha"), new TestCatalogSource("beta"), new TestCatalogSource("gamma")];
        AppCatalogContinuation.SourceCursor[] cursors =
        [
            new("alpha", null, 3, Done: false),
            new("beta", "cursor;with:separators|and unicode \u00e9", 1, Done: false),
            new("gamma", null, 0, Done: true),
        ];

        var token = AppCatalogContinuation.Encode(cursors);

        Assert.That(token, Is.Not.Null);
        Assert.That(AppCatalogContinuation.TryDecode(token!, sources, out var decoded), Is.True);
        Assert.That(decoded, Is.EqualTo(cursors));
    }

    [Test]
    public void AppCatalogContinuation_is_null_when_every_source_is_exhausted() =>
        Assert.That(AppCatalogContinuation.Encode([new("alpha", null, 0, Done: true)]), Is.Null);

    [Test]
    public void AppCatalogContinuation_refuses_a_token_for_other_sources_or_a_tampered_token()
    {
        IAppCatalogSource[] alpha = [new TestCatalogSource("alpha")];
        IAppCatalogSource[] beta = [new TestCatalogSource("beta")];
        var token = AppCatalogContinuation.Encode([new("alpha", "c", 2, Done: false)])!;

        Assert.That(AppCatalogContinuation.TryDecode(token, beta, out _), Is.False);
        Assert.That(AppCatalogContinuation.TryDecode(token + "x", alpha, out _), Is.False);
        Assert.That(AppCatalogContinuation.TryDecode(string.Empty, alpha, out _), Is.False);
        Assert.That(AppCatalogContinuation.TryDecode(new string('A', 20_000), alpha, out _), Is.False);
        Assert.That(AppCatalogContinuation.TryDecode(Encode("1;alpha:-1:n:"), alpha, out _), Is.False, "negative offset");
        Assert.That(AppCatalogContinuation.TryDecode(Encode("1;alpha:0:q:"), alpha, out _), Is.False, "unknown state");
        Assert.That(AppCatalogContinuation.TryDecode(Encode("1;alpha:0:c:"), alpha, out _), Is.False, "cursor state without a cursor");
        Assert.That(AppCatalogContinuation.TryDecode(Encode("1;alpha:0:c:!!"), alpha, out _), Is.False, "undecodable cursor");
        Assert.That(AppCatalogContinuation.TryDecode(Encode("2;alpha:0:n:"), alpha, out _), Is.False, "unknown version");
    }

    private static string Encode(string text) =>
        System.Buffers.Text.Base64Url.EncodeToString(System.Text.Encoding.UTF8.GetBytes(text));

    [Test]
    public void ToEngineConsent_maps_null_to_unchanged_and_empty_to_no_grants()
    {
        Assert.That(AppsPresentationMapping.ToEngineConsent(null), Is.Null);
        Assert.That(AppsPresentationMapping.ToEngineConsent([]), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.That(AppsPresentationMapping.ToEngineConsent(default(System.Collections.Immutable.ImmutableArray<AppUiBridgeGrantDescriptor>)), Is.SameAs(AppUiBridgeRequest.Empty));
    }

    [Test]
    public void ToEngineConsent_rejects_invalid_grants()
    {
        Assert.Throws<ArgumentException>(() => AppsPresentationMapping.ToEngineConsent([null!]));
        Assert.Throws<ArgumentException>(() => AppsPresentationMapping.ToEngineConsent([new AppUiBridgeGrantDescriptor { Operation = "" }]));
        Assert.Throws<ArgumentException>(() => AppsPresentationMapping.ToEngineConsent([new AppUiBridgeGrantDescriptor { Operation = "nav.sync", Tree = "contacts" }]));
        Assert.Throws<ArgumentException>(() => AppsPresentationMapping.ToEngineConsent([new AppUiBridgeGrantDescriptor { Operation = "data.read", Tree = "../x" }]));
    }

    [Test]
    public void ToWireConsent_maps_a_recorded_set_and_preserves_an_absent_one()
    {
        Assert.That(AppsPresentationMapping.ToWireConsent(null), Is.Null);
        Assert.That(AppsPresentationMapping.ToWireConsent(AppUiBridgeRequest.Empty), Is.Empty);
        Assert.That(
            AppsPresentationMapping.ToWireConsent(AppUiBridgeRequest.Create([new AppUiBridgeGrant("data.write", "contacts")])),
            Is.EqualTo(new[] { new AppUiBridgeGrantDescriptor { Operation = "data.write", Tree = "contacts" } }));
    }

    [Test]
    public void ToWirePresentation_carries_every_member_verbatim()
    {
        var presentation = UiTestManifests.WithUi(AppsControlHarness.Manifest()).Presentation!;

        var wire = AppsPresentationMapping.ToWirePresentation(presentation)!;

        Assert.That(AppsPresentationMapping.ToWirePresentation(null), Is.Null);
        Assert.That(wire.DisplayName, Is.EqualTo(presentation.DisplayName));
        Assert.That(wire.Summary, Is.EqualTo(presentation.Summary));
        Assert.That(wire.Description, Is.EqualTo(presentation.Description));
        Assert.That(wire.Categories, Is.EqualTo(presentation.Categories));
        Assert.That(wire.DocumentationUrl, Is.EqualTo(presentation.DocumentationUrl));
        Assert.That(wire.PublisherDisplayName, Is.EqualTo(presentation.PublisherDisplayName));
        Assert.That(AppsPresentationMapping.ToWirePresentation(presentation with { Icon = null, Categories = null })!.Categories, Is.Empty);
    }

    [Test]
    public async Task AppsAssetReader_refuses_an_asset_over_the_per_asset_byte_bound()
    {
        var oversized = new byte[AppUiBundle.MaxAssetBytes + 1];
        var source = new TestCatalogSource("in-image").Publish(AppsControlHarness.Manifest()).WithAsset("big.js", oversized);

        var asset = await AppsAssetReader.OpenAsync(
            source, AppsControlHarness.AppSlugValue, AppsControlHarness.V(AppsControlHarness.Version), "big.js", UiTestManifests.Sha256(oversized),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, CancellationToken.None);

        Assert.That(asset, Is.Null);
    }

    [Test]
    public async Task AppsAssetReader_returns_null_for_an_asset_the_source_does_not_hold()
    {
        var source = new TestCatalogSource("in-image").Publish(AppsControlHarness.Manifest());

        var asset = await AppsAssetReader.OpenAsync(
            source, AppsControlHarness.AppSlugValue, AppsControlHarness.V(AppsControlHarness.Version), "absent.js", new string('0', 64),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, CancellationToken.None);

        Assert.That(asset, Is.Null);
    }
}
