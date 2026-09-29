using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// The catalog is the tool source's only cache, and every advertisement and invocation
/// decision is taken against it. These fixtures drive the paths that surround the build:
/// the epoch short-circuit, the activation failures a bad manifest produces, the
/// can-serve gate, and the tenant resolution each decision is stamped with.
/// </summary>
[TestFixture]
public sealed class AppMcpToolSourceCatalogTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");

    private static LatticeCredential Alice => new("token", principalId: "alice");

    private static AppMcpTestHost NotesHost()
    {
        var host = new AppMcpTestHost();
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search", "found"));
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        return host;
    }

    [Test]
    public async Task GetCatalogAsync_serves_the_built_catalog_while_the_epoch_is_unchanged()
    {
        // The epoch check before the rebuild lock is what keeps a steady-state session off the
        // lock entirely. Every rebuild test advances the epoch first, so the arm that answers
        // without rebuilding is only reached by asking twice for the same one.
        var host = NotesHost();

        var first = await host.ToolSource.GetCatalogAsync(CancellationToken.None);
        var second = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first), "the same epoch must not rebuild");
            Assert.That(host.Source.Resolutions, Is.EqualTo(1), "the app source is not re-read");
            Assert.That(first.Epoch, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetCatalogAsync_rebuilds_once_the_epoch_advances()
    {
        // Anti-vacuity for the case above: a source that never rebuilt would satisfy it, and
        // only an advancing epoch separates "cached" from "frozen".
        var host = NotesHost();
        var first = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        host.Publish(2, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        var second = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.Not.SameAs(first));
            Assert.That(second.Epoch, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task The_installed_app_records_the_tenant_it_was_compiled_for()
    {
        // The tenant a catalog entry was compiled under is the key every advertisement and
        // invocation decision is taken against, so it is read back here rather than only
        // being used to discriminate one entry from another.
        var host = new AppMcpTestHost();
        var acme = TenantId.Parse("acme");
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search"));
        host.Publish(1, AppMcpTestData.Record(acme, Notes, AppMcpTestData.V1));

        var catalog = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.TryGetApp(acme, Notes, out var app), Is.True);
            Assert.That(app!.Tenant, Is.EqualTo(acme));
            Assert.That(catalog.TryGetApp(TenantId.Default, Notes, out _), Is.False);
        });
    }

    [Test]
    public async Task A_non_transient_source_fault_fails_the_activation_rather_than_the_session()
    {
        // A transient backend fault must propagate so the session retries; anything else is the
        // app's own problem and is recorded against that app, leaving the rest of the surface up.
        var host = NotesHost();
        host.Source.Override = _ => throw new InvalidOperationException("the manifest store is corrupt");

        var catalog = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Failures.Single().Failure, Does.Contain("Resolving the manifest failed"));
            Assert.That(catalog.Failures.Single().Failure, Does.Contain(nameof(InvalidOperationException)));
            Assert.That(catalog.TryGetApp(TenantId.Default, Notes, out _), Is.False);
        });
    }

    [Test]
    public async Task A_manifest_identifying_as_another_version_is_refused()
    {
        // The registry names the version; the source is asked for it but answers with a whole
        // manifest, so nothing but this check stops a source serving a different artifact than
        // the one consent was recorded against.
        var host = NotesHost();
        host.Source.Override = _ => AppMcpTestData.Resolved(
            AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V2, "search"));

        var catalog = await host.ToolSource.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Failures.Single().Failure, Does.Contain("identifies as"));
            Assert.That(catalog.Failures.Single().Failure, Does.Contain(AppMcpTestData.V2.Value));
            Assert.That(catalog.TryGetApp(TenantId.Default, Notes, out _), Is.False);
        });
    }

    [Test]
    public async Task A_source_that_cannot_serve_denies_every_invocation()
    {
        // The can-serve gate stands in front of the catalog, so a host that registered the app
        // tool surface without a projection, source or gate denies rather than dereferencing.
        var tool = await FirstToolAsync(NotesHost());
        var unserviceable = new AppMcpToolSource([], NullLogger<AppMcpToolSource>.Instance);

        Assert.That(await unserviceable.IsInvocationPermittedAsync(tool, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task A_tenant_denial_raised_while_re_checking_an_invocation_denies_rather_than_propagates()
    {
        // The re-check runs inside tools/call, where a fail-closed tenant denial must read as
        // "not permitted" rather than escaping as an unhandled fault.
        var host = NotesHost();
        var tool = await FirstToolAsync(host);
        var source = Build(host, new DenyingTenantResolver());

        Assert.That(await source.IsInvocationPermittedAsync(tool, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task An_absent_tenant_resolver_places_every_caller_in_the_default_tenant()
    {
        // Tenancy is optional: with no resolver registered the surface must still serve, and it
        // serves the default tenant rather than refusing.
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = await FirstToolAsync(host);
        var source = Build(host, tenantResolver: null);

        using var credential = LatticeCredentialContext.With(Alice);

        Assert.That(await source.IsInvocationPermittedAsync(tool, CancellationToken.None), Is.True);
    }

    [Test]
    public async Task A_resolver_answering_synchronously_is_not_awaited()
    {
        // The synchronous fast path exists so a tenant-unaware client pays no await per
        // invocation; the shared test resolver only answers asynchronously, so nothing reached it.
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = await FirstToolAsync(host);
        var resolver = new SynchronousTenantResolver(TenantId.Default);
        var source = Build(host, resolver);

        using var credential = LatticeCredentialContext.With(Alice);
        var permitted = await source.IsInvocationPermittedAsync(tool, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(permitted, Is.True);
            Assert.That(resolver.SynchronousCalls, Is.GreaterThan(0));
            Assert.That(resolver.AsynchronousCalls, Is.Zero, "the async overload must not be reached");
        });
    }

    [Test]
    public async Task A_provider_carrying_no_slug_is_ignored_rather_than_grouped()
    {
        // The provider set is host-supplied, so a half-constructed registration must be dropped
        // at composition rather than faulting the whole tool surface.
        var host = NotesHost();
        var providers = new IAppMcpToolProvider[]
        {
            new RawToolProvider(default, AppMcpTestData.Tool("orphan")),
            new AppMcpToolProvider(Notes, [AppMcpTestData.Tool("search", "found")]),
        };
        var source = Build(host, new AmbientTenantResolver(), providers);
        host.Member("alice", "g-readers");

        var catalog = await source.GetCatalogAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.TryGetApp(TenantId.Default, Notes, out var app), Is.True);
            Assert.That(app!.Activation.Tools.Select(t => t.LocalName), Is.EqualTo(new[] { "search" }));
        });
    }

    [Test]
    public async Task A_caller_arriving_during_a_rebuild_reuses_it_rather_than_building_a_second_catalog()
    {
        // The epoch is checked twice: once before the rebuild lock, and again inside it. The
        // outer check serves the steady state; the inner one is what makes a burst of sessions
        // arriving on a fresh epoch cost ONE manifest resolution rather than one each. Only a
        // caller that passed the outer check before the rebuild landed reaches it.
        var projection = new CountingProjection(AppMcpTestData.Snapshot(
            1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1)));
        var appSource = new GatedAppSource(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        var source = new AppMcpToolSource(
            [new AppMcpToolProvider(Notes, [AppMcpTestData.Tool("search")])],
            NullLogger<AppMcpToolSource>.Instance,
            projection,
            appSource,
            new GrantingAccessGate(),
            new CredentialEchoMembershipContext(),
            new AmbientTenantResolver());

        var first = source.GetCatalogAsync(CancellationToken.None).AsTask();
        await appSource.Entered.Task;
        var readsBeforeSecond = projection.CurrentReads;

        // The second caller is only committed to the inner check once it has taken its own
        // pre-lock reading of the epoch, which it did while the first caller still held the
        // lock. After that the ordering no longer matters: whenever it acquires the lock the
        // rebuilt catalog is already published, so it returns from the inner check.
        var second = source.GetCatalogAsync(CancellationToken.None).AsTask();
        await TestPoll.UntilAsync(
            () => projection.CurrentReads > readsBeforeSecond,
            "the second caller to take its pre-lock epoch reading");

        appSource.Release.SetResult();
        var firstCatalog = await first;
        var secondCatalog = await second;

        Assert.Multiple(() =>
        {
            Assert.That(secondCatalog, Is.SameAs(firstCatalog), "the rebuild is shared, not repeated");
            Assert.That(appSource.Resolutions, Is.EqualTo(1), "one epoch costs one manifest resolution");
            Assert.That(firstCatalog.Epoch, Is.EqualTo(1));
        });
    }

    private static async Task<AppMcpNamespacedTool> FirstToolAsync(AppMcpTestHost host)    {
        var catalog = await host.ToolSource.GetCatalogAsync(CancellationToken.None);
        Assert.That(catalog.TryGetApp(TenantId.Default, Notes, out var app), Is.True);
        return app!.Activation.Tools[0];
    }

    private static AppMcpToolSource Build(
        AppMcpTestHost host,
        ITenantContextResolver? tenantResolver,
        IEnumerable<IAppMcpToolProvider>? providers = null)
        => new(
            providers ?? host.Providers,
            NullLogger<AppMcpToolSource>.Instance,
            host.Projection,
            host.Source,
            host.Gate,
            host.Membership,
            tenantResolver);

    private sealed class DenyingTenantResolver : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => throw new LatticeTenantAccessDeniedException("the asserted tenant is not the caller's");
    }

    /// <summary>Counts reads of <see cref="Current"/>, so a caller's pre-lock epoch reading is observable.</summary>
    private sealed class CountingProjection(CompiledAppRegistrySnapshot snapshot) : IAppRegistryProjection
    {
        private int _currentReads;

        public int CurrentReads => Volatile.Read(ref _currentReads);

        public CompiledAppRegistrySnapshot Current
        {
            get
            {
                Interlocked.Increment(ref _currentReads);
                return snapshot;
            }
        }

        public long CurrentEpoch => snapshot.Epoch;

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    /// <summary>An app source that parks inside the rebuild until it is released.</summary>
    private sealed class GatedAppSource(AppManifest manifest) : IAppSource
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Resolutions { get; private set; }

        public async ValueTask<AppSourceResult> ResolveAsync(
            AppSlug slug,
            AppVersion? version = null,
            CancellationToken cancellationToken = default)
        {
            Resolutions++;
            Entered.TrySetResult();
            await Release.Task.ConfigureAwait(false);
            return AppMcpTestData.Resolved(manifest);
        }
    }

    private sealed class SynchronousTenantResolver(TenantId tenant) : ITenantContextResolver
    {
        public int SynchronousCalls { get; private set; }

        public int AsynchronousCalls { get; private set; }

        public bool TryResolveCurrent(out TenantId resolved)
        {
            SynchronousCalls++;
            resolved = tenant;
            return true;
        }

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            AsynchronousCalls++;
            return new ValueTask<TenantId>(tenant);
        }
    }
}
