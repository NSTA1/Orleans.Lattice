using System.Collections.Immutable;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Framing;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing;

/// <summary>
/// The per-launch workspace gate, bundle delivery and verification, and the rule that a
/// cache hit never skips the gate.
/// </summary>
[TestFixture]
public sealed class AppFrameBundleLoaderTests
{
    private static AppFrameBundleLoader Loader(ILatticeAppWorkspace? workspace, AppFrameBundleCache? cache = null) =>
        new(workspace, cache ?? new AppFrameBundleCache(), NullLogger<AppFrameBundleLoader>.Instance);

    [Test]
    public async Task AuthorizeAsync_grants_an_enabled_app_with_a_ui()
    {
        var result = await Loader(Workspace()).AuthorizeAsync(Slug);

        Assert.Multiple(() =>
        {
            Assert.That(result.Launch, Is.Not.Null);
            Assert.That(result.Launch!.Slug, Is.EqualTo(Slug));
            Assert.That(result.Launch.InstallRevision, Is.EqualTo(Revision));
            Assert.That(result.Launch.DisplayName, Is.EqualTo(DisplayName));
            Assert.That(result.Launch.Trees, Is.EquivalentTo(new[] { "orders", "notes" }));
        });
    }

    [Test]
    public async Task AuthorizeAsync_with_no_workspace_registered_is_no_grant()
    {
        var result = await Loader(null).AuthorizeAsync(Slug);
        Assert.That(result, Is.EqualTo(AppFrameLaunchResult.Refused(AppFrameFailure.NoGrant)));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("other")]
    [TestCase("TASKBOARD")]
    public async Task AuthorizeAsync_an_app_not_in_the_callers_list_is_no_grant(string? slug)
    {
        var workspace = Workspace();
        var result = await Loader(workspace).AuthorizeAsync(slug);

        Assert.Multiple(() =>
        {
            Assert.That(result.Launch, Is.Null);
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(workspace.DescribeCalls, Is.Zero);
        });
    }

    [Test]
    public async Task AuthorizeAsync_described_but_not_listed_is_no_grant()
    {
        var workspace = Workspace();
        workspace.Apps.Clear();

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [TestCase(AppLifecycleState.Disabled)]
    [TestCase(AppLifecycleState.Installed)]
    [TestCase(AppLifecycleState.Uninstalled)]
    [TestCase(AppLifecycleState.Failed)]
    public async Task AuthorizeAsync_an_app_that_is_not_enabled_is_no_grant(AppLifecycleState state)
    {
        var workspace = Workspace();
        workspace.Descriptions[Slug] = Describe() with { State = state };

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task AuthorizeAsync_a_revision_that_moved_between_list_and_describe_is_no_grant()
    {
        var workspace = Workspace();
        workspace.Descriptions[Slug] = Describe(revision: Revision + 1);

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task AuthorizeAsync_a_null_description_is_no_grant()
    {
        var workspace = Workspace();
        workspace.Descriptions.Clear();

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task AuthorizeAsync_an_app_without_a_ui_is_no_ui()
    {
        var workspace = new FakeAppWorkspace().Grant(Describe() with { Ui = null });

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoUi));
    }

    [Test]
    public async Task AuthorizeAsync_a_minimum_protocol_above_the_hosts_is_protocol_unsupported()
    {
        var workspace = Workspace(Describe(Ui() with { MinProtocol = AppFrameProtocol.Version + 1 }));

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.ProtocolUnsupported));
    }

    [Test]
    public async Task AuthorizeAsync_a_failing_workspace_is_unavailable()
    {
        var workspace = Workspace();
        workspace.Throw = new InvalidOperationException("secret detail");

        var result = await Loader(workspace).AuthorizeAsync(Slug);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.Unavailable));
    }

    [Test]
    public void AuthorizeAsync_propagates_cancellation()
    {
        var workspace = Workspace();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        workspace.Throw = new OperationCanceledException(cancelled.Token);

        Assert.That(async () => await Loader(workspace).AuthorizeAsync(Slug, cancelled.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task LoadAsync_delivers_every_verified_asset_in_manifest_order()
    {
        var loader = Loader(Workspace());
        var launch = (await loader.AuthorizeAsync(Slug)).Launch;

        var result = await loader.LoadAsync(launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Bundle, Is.Not.Null);
            Assert.That(result.Bundle!.Assets.Select(asset => asset.Path), Is.EqualTo(new[] { Entry, Style, Script }));
            Assert.That(result.Bundle.Assets[0].Bytes.ToArray(), Is.EqualTo(EntryBytes));
            Assert.That(result.Bundle.Assets[2].Digest, Is.EqualTo(Sha(ScriptBytes)));
        });
    }

    [Test]
    public async Task LoadAsync_bytes_that_do_not_match_the_pinned_digest_are_a_digest_mismatch()
    {
        var workspace = Workspace();
        var tampered = Encoding.UTF8.GetBytes("lattice.ready.then(() => steal());");
        workspace.Assets[Script] = new AppUiAsset { Path = Script, MediaType = "text/javascript", Sha256 = Sha(ScriptBytes), Bytes = tampered };
        var cache = new AppFrameBundleCache();
        var loader = Loader(workspace, cache);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.DigestMismatch));
            Assert.That(result.Bundle, Is.Null);
            Assert.That(cache.Count, Is.EqualTo(2), "only the two verified assets were admitted");
        });
    }

    [Test]
    public async Task LoadAsync_an_asset_whose_reported_digest_differs_from_the_manifest_is_a_digest_mismatch()
    {
        var workspace = Workspace();
        var other = Encoding.UTF8.GetBytes("x");
        workspace.Assets[Style] = new AppUiAsset { Path = Style, MediaType = "text/css", Sha256 = Sha(other), Bytes = other };
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.DigestMismatch));
    }

    [Test]
    public async Task LoadAsync_a_declared_bundle_digest_that_does_not_match_the_assets_is_refused_before_any_fetch()
    {
        var workspace = Workspace(Describe(Ui() with { BundleDigest = new string('0', 64) }));
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleDigestMismatch));
            Assert.That(workspace.AssetRequests, Is.Empty);
        });
    }

    [Test]
    public async Task LoadAsync_a_malformed_bundle_digest_is_a_bundle_digest_mismatch()
    {
        var workspace = Workspace(Describe(Ui() with { BundleDigest = "not-a-digest" }));
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleDigestMismatch));
    }

    private static IEnumerable<TestCaseData> MalformedDeclarations()
    {
        AppUiDescriptor Reshape(ImmutableArray<AppUiAssetDescriptor> assets, string? entry = null) =>
            Ui() with { Assets = assets, Entry = entry ?? Entry, BundleDigest = BundleDigest(assets) };

        var assets = AssetDescriptors;
        yield return new TestCaseData(Ui() with { Assets = [] }).SetName("no assets");
        yield return new TestCaseData(Reshape(assets.SetItem(1, assets[1] with { Path = "../app.css" }))).SetName("traversal path");
        yield return new TestCaseData(Reshape(assets.SetItem(1, assets[1] with { Path = "App.css" }))).SetName("upper-case path");
        yield return new TestCaseData(Reshape(assets.SetItem(1, assets[1] with { MediaType = "application/octet-stream" }))).SetName("disallowed media type");
        yield return new TestCaseData(Reshape(assets.Add(assets[1]))).SetName("duplicate path");
        yield return new TestCaseData(Reshape(assets.SetItem(0, assets[0] with { Sha256 = "ABC" }))).SetName("malformed asset digest");
        yield return new TestCaseData(Reshape(assets, entry: Style)).SetName("entry is not html");
        yield return new TestCaseData(Ui() with { Entry = "missing.html" }).SetName("entry not declared");
        yield return new TestCaseData(Ui() with { Styles = [Script] }).SetName("style is not css");
        yield return new TestCaseData(Ui() with { Scripts = [new AppUiScriptDescriptor { Path = Style }] }).SetName("script is not javascript");
        yield return new TestCaseData(Ui() with { Scripts = [new AppUiScriptDescriptor { Path = "other.js" }] }).SetName("script not declared");
        var many = Enumerable.Range(0, AppFrameBundleRules.MaxAssets + 1)
            .Select(i => new AppUiAssetDescriptor { Path = $"a{i}.png", MediaType = "image/png", Sha256 = new string('a', 64) })
            .Prepend(assets[0])
            .ToImmutableArray();
        yield return new TestCaseData(Reshape(many)).SetName("too many assets");
    }

    [TestCaseSource(nameof(MalformedDeclarations))]
    public async Task LoadAsync_a_malformed_declaration_is_refused_before_any_fetch(AppUiDescriptor ui)
    {
        var workspace = Workspace(Describe(ui));
        var loader = Loader(workspace);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch;

        var result = await loader.LoadAsync(launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Bundle, Is.Null);
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
            Assert.That(workspace.AssetRequests, Is.Empty);
        });
    }

    [Test]
    public async Task LoadAsync_an_asset_the_workspace_will_not_serve_is_no_grant()
    {
        var workspace = Workspace();
        workspace.Assets.Remove(Style);
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task LoadAsync_an_asset_served_with_another_media_type_is_invalid()
    {
        var workspace = Workspace();
        workspace.Assets[Script] = workspace.Assets[Script] with { MediaType = "text/html" };
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
    }

    [Test]
    public async Task LoadAsync_an_asset_served_under_another_path_is_invalid()
    {
        var workspace = Workspace();
        workspace.Assets[Script] = workspace.Assets[Script] with { Path = "evil.js" };
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
    }

    [Test]
    public async Task LoadAsync_an_asset_over_the_per_asset_cap_is_invalid()
    {
        var huge = new byte[AppFrameBundleRules.MaxAssetBytes + 1];
        var descriptor = Describe(Ui() with
        {
            Assets = AssetDescriptors.SetItem(2, AssetDescriptors[2] with { Sha256 = Sha(huge) }),
            BundleDigest = BundleDigest(AssetDescriptors.SetItem(2, AssetDescriptors[2] with { Sha256 = Sha(huge) })),
        });
        var workspace = Workspace(descriptor);
        workspace.Assets[Script] = Asset(Script, "text/javascript", huge);
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
    }

    [Test]
    public async Task LoadAsync_a_bundle_over_the_total_cap_is_invalid()
    {
        var block = new byte[AppFrameBundleRules.MaxAssetBytes];
        var images = Enumerable.Range(0, 9).Select(i =>
        {
            var bytes = (byte[])block.Clone();
            bytes[0] = (byte)i;
            return (Path: $"img{i}.png", Bytes: bytes);
        }).ToArray();
        var assets = AssetDescriptors.AddRange(images.Select(image =>
            new AppUiAssetDescriptor { Path = image.Path, MediaType = "image/png", Sha256 = Sha(image.Bytes) }));
        var workspace = Workspace(Describe(Ui() with { Assets = assets, BundleDigest = BundleDigest(assets) }));
        foreach (var image in images)
        {
            workspace.Assets[image.Path] = Asset(image.Path, "image/png", image.Bytes);
        }

        var loader = Loader(workspace);
        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
    }

    [TestCase("<script>alert(1)</script>")]
    [TestCase("<html><body>x</body></html>")]
    [TestCase("<HEAD>")]
    public async Task LoadAsync_an_entry_fragment_F1_would_refuse_is_invalid(string fragment)
    {
        var bytes = Encoding.UTF8.GetBytes(fragment);
        var assets = AssetDescriptors.SetItem(0, AssetDescriptors[0] with { Sha256 = Sha(bytes) });
        var workspace = Workspace(Describe(Ui() with { Assets = assets, BundleDigest = BundleDigest(assets) }));
        workspace.Assets[Entry] = Asset(Entry, "text/html", bytes);
        var loader = Loader(workspace);

        var result = await loader.LoadAsync((await loader.AuthorizeAsync(Slug)).Launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.BundleInvalid));
    }

    [Test]
    public async Task LoadAsync_a_null_launch_is_no_grant()
    {
        var result = await Loader(Workspace()).LoadAsync(null);
        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
    }

    [Test]
    public async Task LoadAsync_a_launch_issued_by_another_circuit_is_no_grant_and_touches_nothing()
    {
        var shared = new AppFrameBundleCache();
        var other = Loader(Workspace(), shared);
        var launch = (await other.AuthorizeAsync(Slug)).Launch;
        await other.LoadAsync(launch);

        var workspace = Workspace();
        var result = await Loader(workspace, shared).LoadAsync(launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(workspace.AssetRequests, Is.Empty);
        });
    }

    [Test]
    public async Task LoadAsync_a_failing_workspace_is_unavailable()
    {
        var workspace = Workspace();
        var loader = Loader(workspace);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch;
        workspace.Throw = new InvalidOperationException("boom");

        var result = await loader.LoadAsync(launch);

        Assert.That(result.Failure, Is.EqualTo(AppFrameFailure.Unavailable));
    }

    [Test]
    public async Task A_cache_hit_saves_the_fetch_for_a_launch_that_passed_the_gate()
    {
        var shared = new AppFrameBundleCache();
        var first = Loader(Workspace(), shared);
        await first.LoadAsync((await first.AuthorizeAsync(Slug)).Launch);

        var workspace = Workspace();
        var second = Loader(workspace, shared);
        var result = await second.LoadAsync((await second.AuthorizeAsync(Slug)).Launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Bundle, Is.Not.Null);
            Assert.That(workspace.ListCalls, Is.EqualTo(1), "the gate still ran");
            Assert.That(workspace.AssetRequests, Is.Empty, "every asset came from the cache");
        });
    }

    [Test]
    public async Task A_cache_hit_never_skips_the_gate_for_a_user_without_a_grant()
    {
        var shared = new AppFrameBundleCache();
        var granted = Loader(Workspace(), shared);
        await granted.LoadAsync((await granted.AuthorizeAsync(Slug)).Launch);
        Assert.That(shared.Count, Is.EqualTo(3), "the cache is warm");

        var denied = new FakeAppWorkspace();
        var loader = Loader(denied, shared);
        var launch = await loader.AuthorizeAsync(Slug);
        var load = await loader.LoadAsync(launch.Launch);

        Assert.Multiple(() =>
        {
            Assert.That(launch.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(load.Bundle, Is.Null);
            Assert.That(load.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(denied.AssetRequests, Is.Empty);
        });
    }

    [Test]
    public async Task The_cache_is_keyed_by_bundle_digest_so_an_upgrade_refetches()
    {
        var shared = new AppFrameBundleCache();
        var first = Loader(Workspace(), shared);
        await first.LoadAsync((await first.AuthorizeAsync(Slug)).Launch);

        var bytes = Encoding.UTF8.GetBytes("<main>v2</main>");
        var assets = AssetDescriptors.SetItem(0, AssetDescriptors[0] with { Sha256 = Sha(bytes) });
        var workspace = Workspace(Describe(Ui() with { Assets = assets, BundleDigest = BundleDigest(assets) }, version: "2.0.0"));
        workspace.Assets[Entry] = Asset(Entry, "text/html", bytes);
        var second = Loader(workspace, shared);

        var result = await second.LoadAsync((await second.AuthorizeAsync(Slug)).Launch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Bundle!.Assets[0].Bytes.ToArray(), Is.EqualTo(bytes));
            Assert.That(workspace.AssetRequests, Is.EqualTo(new[] { Entry, Style, Script }));
        });
    }

    [Test]
    public async Task GetRevocationAsync_a_current_launch_is_not_revoked()
    {
        var workspace = Workspace();
        var loader = Loader(workspace);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch!;

        Assert.That(await loader.GetRevocationAsync(launch), Is.Null);
    }

    [Test]
    public async Task GetRevocationAsync_names_why_a_launch_no_longer_holds()
    {
        var workspace = Workspace();
        var loader = Loader(workspace);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch!;

        async Task<string?> With(WorkspaceAppDescriptor? descriptor)
        {
            workspace.Descriptions.Clear();
            if (descriptor is not null)
            {
                workspace.Descriptions[Slug] = descriptor;
            }

            return await loader.GetRevocationAsync(launch);
        }

        var reasons = new[]
        {
            await With(null),
            await With(Describe() with { State = AppLifecycleState.Disabled }),
            await With(Describe() with { State = AppLifecycleState.Uninstalled }),
            await With(Describe() with { State = AppLifecycleState.Failed }),
            await With(Describe(version: "2.0.0", revision: Revision + 1)),
            await With(Describe(revision: Revision + 1)),
        };

        Assert.That(reasons, Is.EqualTo(new[] { "closed", "disabled", "uninstalled", "closed", "upgraded", "revision" }));
    }

    [Test]
    public async Task GetRevocationAsync_a_launch_from_another_circuit_is_closed()
    {
        var launch = (await Loader(Workspace()).AuthorizeAsync(Slug)).Launch!;
        Assert.That(await Loader(Workspace()).GetRevocationAsync(launch), Is.EqualTo("closed"));
    }

    [Test]
    public async Task GetRevocationAsync_an_unreachable_workspace_does_not_revoke()
    {
        var workspace = Workspace();
        var loader = Loader(workspace);
        var launch = (await loader.AuthorizeAsync(Slug)).Launch!;
        workspace.Throw = new InvalidOperationException();

        Assert.That(await loader.GetRevocationAsync(launch), Is.Null);
    }

    [Test]
    public void GetRevocationAsync_rejects_a_null_launch()
    {
        Assert.That(async () => await Loader(Workspace()).GetRevocationAsync(null!), Throws.ArgumentNullException);
    }
}
