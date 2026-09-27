using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <c>AppInstall</c> gating: every lifecycle transition authorizes the caller for the
/// scopeless capability over the cluster-wide sentinel before any storage access, a
/// denial or key-filtered allow throws without touching the store, and trusted
/// system-origin infrastructure skips the check.
/// </summary>
public sealed partial class AppRegistryTests
{
    private static readonly string[] AllTransitions = { "install", "upgrade", "enable", "disable", "uninstall" };

    private static Task<AppRegistryTransitionResult> RunTransition(AppRegistry registry, string transition) => transition switch
    {
        "install" => registry.InstallAsync(AppRegistryTestData.Request()),
        "upgrade" => registry.UpgradeAsync(AppRegistryTestData.Request()),
        "enable" => registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug),
        "disable" => registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug),
        _ => registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug),
    };

    [TestCaseSource(nameof(AllTransitions))]
    public async Task Every_transition_authorizes_AppInstall_over_the_cluster_wide_sentinel(string transition)
    {
        var gate = RecordingAccessGate.AllowAll();
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore(), gate, new FakeMembership(new LatticeSubject("operator")));

        await RunTransition(registry, transition);

        Assert.That(gate.Requests, Has.Count.EqualTo(1));
        var request = gate.Requests[0];
        Assert.That(request.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
        Assert.That(request.TreeId, Is.EqualTo(LatticeScope.ClusterWide().TreeId));
        Assert.That(request.Operation, Is.EqualTo(LatticeOperation.AppInstall));
        Assert.That(request.Key, Is.Null);
        Assert.That(request.Subject.SubjectId, Is.EqualTo("operator"));
        Assert.That(gate.SystemOriginObserved[0], Is.False, "the caller is authorized as itself, not as system-origin");
    }

    [TestCaseSource(nameof(AllTransitions))]
    public async Task A_denied_transition_throws_and_never_touches_the_store(string transition)
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store, RecordingAccessGate.DenyAll(), new FakeMembership(new LatticeSubject("mallory")));

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => RunTransition(registry, transition));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.AppInstall));
        Assert.That(ex.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
        Assert.That(ex.SubjectId, Is.EqualTo("mallory"));
        Assert.That(store.Reads, Is.EqualTo(0), "authorization precedes every read");
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public void A_key_filtered_allow_does_not_authorize_AppInstall()
    {
        var store = new InMemoryAppRegistryStore();
        var gate = new RecordingAccessGate(_ => LatticeAccessDecision.Filtered(_ => true));
        var registry = AppRegistryTestData.CreateRegistry(store, gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => registry.InstallAsync(AppRegistryTestData.Request()));
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task System_origin_callers_skip_the_gate_and_record_no_consenting_subject()
    {
        var gate = RecordingAccessGate.DenyAll();
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore(), gate);

        AppRegistryTransitionResult result;
        using (LatticeSystemOrigin.Enter())
        {
            result = await registry.InstallAsync(AppRegistryTestData.Request());
        }

        Assert.That(result.Succeeded, Is.True);
        Assert.That(gate.Requests, Is.Empty);
        Assert.That(result.Record!.ConsentedBy, Is.Null);
    }

    [Test]
    public async Task The_authorized_subject_is_recorded_as_the_consenting_principal()
    {
        var registry = AppRegistryTestData.CreateRegistry(
            new InMemoryAppRegistryStore(), RecordingAccessGate.AllowAll(), new FakeMembership(new LatticeSubject("operator")));

        var result = await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(result.Record!.ConsentedBy, Is.EqualTo("operator"));
    }

    [Test]
    public async Task An_uncached_subject_is_resolved_under_system_origin_then_authorized_as_itself()
    {
        var gate = RecordingAccessGate.AllowAll();
        var membership = new FakeMembership(new LatticeSubject("operator"), resolveSynchronously: false);
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore(), gate, membership);

        await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(membership.AsyncResolutions, Is.EqualTo(1));
        Assert.That(membership.ResolvedUnderSystemOrigin, Is.True, "directory reads must not re-enter the gate");
        Assert.That(gate.Requests[0].Subject.SubjectId, Is.EqualTo("operator"));
        Assert.That(gate.SystemOriginObserved[0], Is.False);
    }

    [Test]
    public async Task Without_a_membership_context_the_caller_is_anonymous()
    {
        var gate = RecordingAccessGate.AllowAll();
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore(), gate);

        await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(gate.Requests[0].Subject.IsAnonymous, Is.True);
    }

    [Test]
    public async Task Reads_do_not_consult_the_gate()
    {
        var gate = RecordingAccessGate.DenyAll();
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled));
        var registry = AppRegistryTestData.CreateRegistry(store, gate);

        Assert.That(await registry.GetAsync(TenantId.Default, AppRegistryTestData.Slug), Is.Not.Null);
        Assert.That(await registry.ListAsync().ToListAsync(), Has.Count.EqualTo(1));
        Assert.That(await registry.ListForTenantAsync(TenantId.Default).ToListAsync(), Has.Count.EqualTo(1));
        Assert.That(gate.Requests, Is.Empty);
    }

    [Test]
    public void AppInstallAuthorizer_null_gate_throws()
    {
        Assert.That(() => new AppInstallAuthorizer(null!), Throws.ArgumentNullException);
    }

    /// <summary>A membership context resolving a fixed subject, synchronously or asynchronously.</summary>
    private sealed class FakeMembership(LatticeSubject subject, bool resolveSynchronously = true) : ILatticeMembershipContext
    {
        public int AsyncResolutions { get; private set; }

        public bool ResolvedUnderSystemOrigin { get; private set; }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            AsyncResolutions++;
            ResolvedUnderSystemOrigin = LatticeSystemOrigin.IsActive;
            return new ValueTask<LatticeSubject>(subject);
        }

        public bool TryResolveCurrent(out LatticeSubject resolved)
        {
            resolved = resolveSynchronously ? subject : default;
            return resolveSynchronously;
        }
    }
}
