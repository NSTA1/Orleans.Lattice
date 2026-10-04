using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The defensive arms of the smaller app-activation collaborators: the startup
/// reconciler's unconditional fault handler, the in-image catalog's skip arms, the
/// tree provisioner's absent-tree short circuit, the dependant-app scan, and the
/// scope-coverage and rule-id fall-throughs for an unrecognised scope kind.
/// </summary>
/// <remarks>
/// These arms exist so that one malformed registration, one unreachable tree, or
/// one enum value from a newer wire format cannot take down a silo or silently
/// widen a grant. None of them is on a happy path, so each is asserted against the
/// specific consequence it prevents rather than merely being executed.
/// </remarks>
[TestFixture]
public sealed class AppActivationDefensiveArmTests
{
    private sealed class StaticOptionsMonitor(LatticeAppsOptions options) : IOptionsMonitor<LatticeAppsOptions>
    {
        public LatticeAppsOptions CurrentValue { get; } = options;

        public LatticeAppsOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeAppsOptions, string?> listener) => null;
    }

    private sealed class ThrowingOptionsMonitor(Exception fault) : IOptionsMonitor<LatticeAppsOptions>
    {
        public LatticeAppsOptions CurrentValue => throw fault;

        public LatticeAppsOptions Get(string? name) => throw fault;

        public IDisposable? OnChange(Action<LatticeAppsOptions, string?> listener) => null;
    }

    // ----- the startup reconciler must never stop the host -----

    [Test]
    public async Task A_fault_reading_the_reconcile_settings_never_stops_the_host_at_startup()
    {
        // The catch is deliberately unconditional: the reconciler is a hosted
        // service, so an escaping exception from ExecuteAsync faults the host's
        // start. A silo that will not boot because an options binding threw is a far
        // worse outcome than apps keeping their last applied state.
        var registry = Substitute.For<IAppRegistry>();
        var reconciler = new AppStartupReconciler(
            registry,
            Substitute.For<IAppActivationPipeline>(),
            new ThrowingOptionsMonitor(new InvalidOperationException("options unavailable")),
            NullLogger<AppStartupReconciler>.Instance);

        await reconciler.StartAsync(CancellationToken.None);
        await reconciler.ExecuteTask!;

        Assert.Multiple(() =>
        {
            Assert.That(reconciler.ExecuteTask.IsCompletedSuccessfully, Is.True,
                "a reconcile fault must be swallowed, not surfaced as a failed host start");
            Assert.That(reconciler.ExecuteTask.IsFaulted, Is.False);
        });
        registry.DidNotReceiveWithAnyArgs().ListAsync(default);
        await reconciler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Reconcile_on_startup_disabled_runs_nothing()
    {
        // The accepting counterpart to the arm above: the early return must skip the
        // registry scan entirely rather than reaching it and being swallowed, which
        // would be indistinguishable from the outside.
        var registry = Substitute.For<IAppRegistry>();
        var reconciler = new AppStartupReconciler(
            registry,
            Substitute.For<IAppActivationPipeline>(),
            new StaticOptionsMonitor(new LatticeAppsOptions { ReconcileOnStartup = false }),
            NullLogger<AppStartupReconciler>.Instance);

        await reconciler.StartAsync(CancellationToken.None);
        await reconciler.ExecuteTask!;

        registry.DidNotReceiveWithAnyArgs().ListAsync(default);
        await reconciler.StopAsync(CancellationToken.None);
    }

    // ----- the in-image manifest catalog skips what it cannot use -----

    private static InImageAppManifestCatalog Catalog(InImageAppSourceOptions options, IAppSource source) =>
        new(Options.Create(options), source);

    [Test]
    public void The_catalog_skips_a_null_registration_and_keeps_the_rest()
    {
        // Registrations come from host configuration, where a null entry is an
        // ordinary configuration slip. One of them must not deny every app in the
        // image its manifest.
        var manifest = ActivationHarness.Manifest();
        var source = new ActivationAppSource();
        source.Publish(manifest);
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(null!);
        options.Registrations.Add(new InImageAppRegistration(manifest.Identity.Slug, typeof(object).Assembly, "unused.json"));

        var catalog = Catalog(options, source);

        Assert.That(catalog.Manifests.Select(m => m.Identity.Slug), Is.EqualTo(new[] { manifest.Identity.Slug }));
    }

    [Test]
    public void The_catalog_loads_a_duplicated_slug_only_once()
    {
        var manifest = ActivationHarness.Manifest();
        var source = new ActivationAppSource();
        source.Publish(manifest);
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(new InImageAppRegistration(manifest.Identity.Slug, typeof(object).Assembly, "unused.json"));
        options.Registrations.Add(new InImageAppRegistration(manifest.Identity.Slug, typeof(object).Assembly, "unused.json"));

        var catalog = Catalog(options, source);

        Assert.That(catalog.Manifests, Has.Count.EqualTo(1),
            "a slug registered twice must yield one manifest, not a duplicate route into the same app");
    }

    [Test]
    public void The_catalog_skips_a_source_that_does_not_resolve_synchronously()
    {
        // The catalog is built lazily on a synchronous property, so it cannot await.
        // A source that resolves asynchronously is skipped rather than blocked on -
        // blocking here would deadlock the first caller to touch the catalog.
        var manifest = ActivationHarness.Manifest();
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(new InImageAppRegistration(manifest.Identity.Slug, typeof(object).Assembly, "unused.json"));

        var catalog = Catalog(options, new AsynchronousAppSource(manifest));

        Assert.That(catalog.Manifests, Is.Empty);
    }

    /// <summary>An <see cref="IAppSource"/> that never completes synchronously.</summary>
    private sealed class AsynchronousAppSource(AppManifest manifest) : IAppSource
    {
        public async ValueTask<AppSourceResult> ResolveAsync(
            AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return ActivationAppSource.ResolvedResult(manifest);
        }
    }

    // ----- scope coverage and rule-id derivation reject an unknown scope kind -----

    private const LatticeScopeKind UnknownScopeKind = (LatticeScopeKind)99;

    [Test]
    public void An_approved_exception_of_an_unknown_scope_kind_covers_nothing()
    {
        // The ceiling's exception list is what widens an app's reach beyond its own
        // trees. An exception whose kind this build does not recognise must cover
        // nothing: treating it as covering would grant a scope nobody consented to,
        // and a record written by a newer build is exactly where such a kind comes
        // from.
        const string TreeId = "a/billing/invoices";
        var requested = new LatticeScope(LatticeScopeKind.Tree, TreeId);
        var unknownException = new[] { new LatticeScope(UnknownScopeKind, TreeId) };
        var knownException = new[] { new LatticeScope(LatticeScopeKind.Tree, TreeId) };

        Assert.Multiple(() =>
        {
            Assert.That(AppScopeCoverage.IsCovered(requested, unknownException), Is.False,
                "an unrecognised exception kind must fail closed");
            Assert.That(AppScopeCoverage.IsCovered(requested, knownException), Is.True,
                "positive control: a recognised exception over the same tree does approve it");
        });
    }

    [Test]
    public void Deriving_a_rule_id_for_an_unknown_scope_kind_is_refused()
    {
        // A rule id is the identity a grant is stored and diffed under. Deriving one
        // from an unrecognised kind would let two different scopes collapse onto the
        // same id, so the derivation refuses rather than inventing a label.
        var unknown = new LatticeScope(UnknownScopeKind, "a/notes/records");

        Assert.That(
            () => AppRoleCompiler.ComputeRuleId(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse("notes")), "notes", "reader", "readers", unknown),
            Throws.ArgumentException.With.Message.Contains("Unknown scope kind"));

        // Positive control: a recognised kind derives an id over the same inputs.
        Assert.That(
            AppRoleCompiler.ComputeRuleId(AppRoleCompiler.GetOwnedRuleIdPrefix(AppSlug.Parse("notes")), "notes", "reader", "readers", new LatticeScope(LatticeScopeKind.Tree, "a/notes/records")),
            Is.Not.Empty);
    }

    // ----- the manifest parser strips a UTF-8 byte-order mark -----

    [Test]
    public void A_manifest_stream_with_a_byte_order_mark_parses()
    {
        // A BOM is what a Windows editor writes by default, so a manifest authored
        // that way must not read as malformed JSON.
        var json = """
            {"identity":{"slug":"notes","version":"1.0.0"},"trees":[],"roles":[],"subscriptions":[],"mcpTools":[]}
            """;
        using var withBom = new MemoryStream([.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes(json)]);
        using var without = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var fromBom = AppManifestParser.Parse(withBom);
        var plain = AppManifestParser.Parse(without);

        Assert.Multiple(() =>
        {
            Assert.That(fromBom.IsValid, Is.True, () => string.Join("; ", fromBom.Errors.Select(e => e.Message)));
            Assert.That(plain.IsValid, Is.True);
            Assert.That(fromBom.Manifest!.Identity.Slug, Is.EqualTo(plain.Manifest!.Identity.Slug),
                "a byte-order mark must not change what the manifest parses to");
        });
    }
}
