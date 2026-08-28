using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Cross-cutting end-to-end tests for the runtime replication-configuration
/// feature. Unlike the per-seam unit fixtures (which exercise the authority, the
/// compiled-snapshot maintainer, the resolver, and the membership in isolation),
/// these wire the <b>whole flow together</b> over a single shared config store
/// that converges writes through the OR-Map merge exactly as the dogfooded
/// <c>sys-replication-config</c> tree does: the real
/// <see cref="LatticeReplicationConfigAuthority"/> authors an enable/disable, the
/// real <see cref="CompiledReplicationConfigSnapshotMaintainer"/> compiles a
/// snapshot from that same store, and the real
/// <see cref="SnapshotReplicatedTreeMembership"/> /
/// <see cref="SnapshotLatticeMergeModeResolver"/> read that snapshot - so an
/// assertion at the read end proves the authored change propagated the length of
/// the pipeline.
/// <para>
/// Every test here is a deterministic in-memory wiring test (no cluster, no
/// sleeps): the maintainer is warmed and rebuilt synchronously via
/// <see cref="CompiledReplicationConfigSnapshotMaintainer.EnsureWarmAsync"/> /
/// <see cref="CompiledReplicationConfigSnapshotMaintainer.RebuildNowAsync"/>, so
/// each read observes a fixed epoch. The genuinely multi-cluster convergence
/// scenario lives in the sibling
/// <see cref="RuntimeReplicationConfigCrossClusterEndToEndTests"/> fixture under
/// the <c>Integration</c> category.
/// </para>
/// </summary>
[TestFixture]
public sealed class RuntimeReplicationConfigEndToEndTests
{
    private const string SiteA = "site-a";
    private const string SiteB = "site-b";
    private const string Tree = "orders";

    // ── Scenario 1: single-writer ENABLE flows end to end ────────────────

    [Test]
    public async Task Enable_authored_via_authority_flows_to_membership_and_resolver()
    {
        var store = new ConvergingConfigStore();
        var authority = CreateAuthority(store);

        // Compile a snapshot over the (empty) store and stand up the read seams.
        var maintainer = await WarmMaintainerAsync(store);
        var membership = Membership(maintainer, staticSeed: null);
        var resolver = Resolver(maintainer, staticSeed: null);

        // Before any enable the tree is neither replicated nor mode-resolvable.
        Assert.Multiple(() =>
        {
            Assert.That(membership.IsReplicated(Tree), Is.False);
            Assert.That(resolver.Resolve(Tree), Is.Null);
        });

        // Author the enable through the engine authority, then rebuild the
        // snapshot the read seams share.
        var result = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(membership.IsReplicated(Tree), Is.True, "the enabled tree must now be replicated");
            Assert.That(membership.ReplicatedTrees, Does.Contain(Tree));
            Assert.That(resolver.Resolve(Tree), Is.EqualTo(LatticeMergeMode.OrSet),
                "the resolver must report the enabled mode authored on the config tree");
            Assert.That(maintainer.Current.TryGetTree(Tree, out var projection), Is.True);
            Assert.That(projection.Enabled, Is.True);
        });
    }

    // ── Scenario 2: enabling a non-empty tree composes a bootstrap ───────

    [Test]
    public async Task Enable_on_non_empty_tree_requests_bootstrap_and_still_converges_config()
    {
        var store = new ConvergingConfigStore();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var authority = CreateAuthority(store, admin: admin, probe: FixedProbe(hasContent: true));

        var result = await authority.EnableReplicationAsync(
            Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: SiteB);

        // The pre-existing rows are pulled once via the receiver-driven snapshot
        // path (the change feed only carries new mutations from here).
        await admin.Received(1).RequestSnapshotAsync(Tree, SiteB, Arg.Any<CancellationToken>());

        // And the config change itself still propagates the length of the
        // pipeline: the read seams see the enabled tree and its mode.
        var maintainer = await WarmMaintainerAsync(store);
        var membership = Membership(maintainer, staticSeed: null);
        var resolver = Resolver(maintainer, staticSeed: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.BootstrapRequested, Is.True);
            Assert.That(membership.IsReplicated(Tree), Is.True);
            Assert.That(resolver.Resolve(Tree), Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    // ── Scenario 4: two divergent enables converge to an ambiguous mode ──

    [Test]
    public async Task Concurrent_divergent_mode_enables_converge_ambiguous_and_pause_shipping()
    {
        // Two clusters that never observed one another each enable the same tree
        // under a different mode. Author each on its own store through the real
        // authority so the divergent MvRegister dots are genuinely minted.
        var storeA = new ConvergingConfigStore();
        var storeB = new ConvergingConfigStore();
        await CreateAuthority(storeA, localReplicaId: SiteA)
            .EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        await CreateAuthority(storeB, localReplicaId: SiteB)
            .EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        // Converge both authored entries into a third store, exactly as the
        // OR-Map delivery would fold two concurrent same-key writes.
        var converged = new ConvergingConfigStore();
        await converged.DeliverAsync(Tree, (await storeA.ReadEntryAsync(Tree))!);
        await converged.DeliverAsync(Tree, (await storeB.ReadEntryAsync(Tree))!);

        var maintainer = await WarmMaintainerAsync(converged);
        // A static seed even offers a mode; ambiguity must still fail closed.
        var resolver = Resolver(maintainer, staticSeed: new() { [Tree] = LatticeMergeMode.LwwRegister });
        var membership = Membership(maintainer, staticSeed: null);

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TryGetTree(Tree, out var projection), Is.True);
            Assert.That(projection.Ambiguous, Is.True, "two live modes must project as ambiguous");
            Assert.That(projection.Mode, Is.Null, "an ambiguous mode must never resolve to a single value");
            Assert.That(projection.Enabled, Is.True);

            // Fail-closed: the resolver returns null so the shipper pauses the
            // tree rather than silently picking a mode and dead-lettering the
            // loser's data, even though a static fallback mode is available.
            Assert.That(resolver.Resolve(Tree), Is.Null,
                "an ambiguous runtime mode must pause shipping, never silently pick a mode");
        });

        // The engine's own status projection agrees the tree is ambiguous.
        var status = await CreateAuthority(converged).GetTreeStatusAsync(Tree);
        Assert.That(status!.Value.Ambiguous, Is.True);
    }

    // ── Scenario 5: in-place mode change is rejected; disable-then-re-enable is the path ──

    [Test]
    public async Task In_place_mode_change_is_rejected_but_disable_then_reenable_succeeds()
    {
        var store = new ConvergingConfigStore();
        var authority = CreateAuthority(store);

        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        // Attempting to change the mode in place on the already-enabled tree is
        // rejected: the change would silently reinterpret every already-shipped
        // value under a new merge algebra.
        var rejection = Assert.ThrowsAsync<LatticeReplicationModeChangeRejectedException>(
            async () => await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister));
        Assert.Multiple(() =>
        {
            Assert.That(rejection!.CurrentMode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(rejection.RequestedMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(rejection.CurrentModeAmbiguous, Is.False);
        });

        // The rejected change left the pipeline untouched: still OrSet downstream.
        var afterReject = await WarmMaintainerAsync(store);
        Assert.That(Resolver(afterReject, null).Resolve(Tree), Is.EqualTo(LatticeMergeMode.OrSet));

        // The sanctioned path - disable then re-enable under the new mode -
        // succeeds and flows the new mode the length of the pipeline.
        await authority.DisableReplicationAsync(Tree);
        var reEnable = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        var afterReEnable = await WarmMaintainerAsync(store);
        var membership = Membership(afterReEnable, staticSeed: null);
        var resolver = Resolver(afterReEnable, staticSeed: null);
        Assert.Multiple(() =>
        {
            Assert.That(reEnable.AlreadyEnabled, Is.False);
            Assert.That(reEnable.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(membership.IsReplicated(Tree), Is.True);
            Assert.That(resolver.Resolve(Tree), Is.EqualTo(LatticeMergeMode.LwwRegister),
                "the re-enabled mode must now flow through the compiled snapshot");
        });
    }

    // ── Scenario 6: the facade authorizes fail-closed before authoring ───

    [Test]
    public void Anonymous_enable_is_denied_and_never_touches_the_config_store()
    {
        var store = new ConvergingConfigStore();
        var control = new E2EReplicationControl(CreateAuthority(store), new DenyAnonymousGate());

        Assert.That(
            async () => await control.EnableAsync(Tree, LatticeMergeMode.OrSet, LatticeSubject.Anonymous),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        // Fail-closed: the denied caller never reached the authority, so no dot
        // was authored and the config tree is unchanged.
        Assert.That(store.WriteCount, Is.EqualTo(0), "a denied enable must not author any config write");
    }

    [Test]
    public void Anonymous_disable_is_denied_before_reaching_the_authority()
    {
        var store = new ConvergingConfigStore();
        var control = new E2EReplicationControl(CreateAuthority(store), new DenyAnonymousGate());

        Assert.That(
            async () => await control.DisableAsync(Tree, LatticeSubject.Anonymous),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(store.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public async Task Authorized_enable_flows_through_to_the_read_seams()
    {
        var store = new ConvergingConfigStore();
        var authority = CreateAuthority(store);
        var control = new E2EReplicationControl(authority, new AllowAllGate());

        // A caller the gate grants passes the authorization choke point and the
        // enable authors normally.
        var result = await control.EnableAsync(Tree, LatticeMergeMode.OrSet, LatticeSubject.Anonymous);

        var maintainer = await WarmMaintainerAsync(store);
        Assert.Multiple(() =>
        {
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(store.WriteCount, Is.EqualTo(1));
            Assert.That(Membership(maintainer, null).IsReplicated(Tree), Is.True);
            Assert.That(Resolver(maintainer, null).Resolve(Tree), Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public async Task GetConfig_is_permission_scoped_and_hides_trees_outside_the_grant()
    {
        var store = new ConvergingConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync("orders", LatticeMergeMode.OrSet);
        await authority.EnableReplicationAsync("inventory", LatticeMergeMode.LwwRegister);

        // The caller is granted only "orders".
        var control = new E2EReplicationControl(authority, new TreeScopedGate("orders"));

        var visible = await control.GetConfigAsync(LatticeSubject.Anonymous);

        Assert.Multiple(() =>
        {
            Assert.That(visible.Select(e => e.TreeId), Is.EquivalentTo(new[] { "orders" }),
                "a tree outside the caller's grant must be filtered out of the config listing");
            Assert.That(visible.Single().Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    // ── Shared wiring ────────────────────────────────────────────────────

    private static LatticeReplicationConfigAuthority CreateAuthority(
        ConvergingConfigStore store,
        string localReplicaId = SiteA,
        ILatticeReplicationAdmin? admin = null,
        ILatticeTreeContentProbe? probe = null)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.LocalReplicaId.Returns(localReplicaId);
        context.IsReplicationEnabled.Returns(!string.IsNullOrEmpty(localReplicaId));

        var preconditions = new LatticeReplicationPreconditionValidator(context);
        admin ??= Substitute.For<ILatticeReplicationAdmin>();
        probe ??= FixedProbe(hasContent: false);

        return new LatticeReplicationConfigAuthority(store, preconditions, context, admin, probe);
    }

    private static ILatticeTreeContentProbe FixedProbe(bool hasContent)
    {
        var probe = Substitute.For<ILatticeTreeContentProbe>();
        probe.HasContentAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(hasContent));
        return probe;
    }

    private static async Task<CompiledReplicationConfigSnapshotMaintainer> WarmMaintainerAsync(
        ILatticeReplicationConfigStore store)
    {
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return maintainer;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> BuildMonitor(
        IReadOnlyDictionary<string, LatticeMergeMode>? staticSeed)
    {
        var options = new LatticeReplicationOptions { ClusterId = "x", ReplicatedTrees = staticSeed };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>())
            .Returns(Substitute.For<IDisposable>());
        return monitor;
    }

    private static SnapshotReplicatedTreeMembership Membership(
        CompiledReplicationConfigSnapshotMaintainer maintainer,
        Dictionary<string, LatticeMergeMode>? staticSeed) =>
        new(maintainer, BuildMonitor(staticSeed));

    private static SnapshotLatticeMergeModeResolver Resolver(
        CompiledReplicationConfigSnapshotMaintainer maintainer,
        Dictionary<string, LatticeMergeMode>? staticSeed) =>
        new(maintainer, new ConfiguredLatticeMergeModeResolver(BuildMonitor(staticSeed)));

    /// <summary>
    /// A minimal <see cref="ILatticeReplicationConfigStore"/> that converges
    /// writes through <see cref="LatticeReplicationConfigEntry.MergeFrom"/> just
    /// as the real OR-Map does, so authoring read-modify-write cycles behave
    /// identically without a cluster. Also counts writes so the authorization
    /// tests can assert a denied caller never reached the authority.
    /// </summary>
    private sealed class ConvergingConfigStore : ILatticeReplicationConfigStore
    {
        private readonly Dictionary<string, LatticeReplicationConfigEntry> _entries =
            new(StringComparer.Ordinal);

        /// <summary>The number of <see cref="WriteEntryAsync"/> calls observed.</summary>
        public int WriteCount { get; private set; }

        /// <summary>Folds an entry in without counting it as an authored write (models delivery).</summary>
        public Task DeliverAsync(string treeId, LatticeReplicationConfigEntry entry)
        {
            Merge(treeId, entry);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default)
        {
            var snapshot = new Dictionary<string, LatticeReplicationConfigEntry>(StringComparer.Ordinal);
            foreach (var pair in _entries)
            {
                snapshot[pair.Key] = pair.Value.Clone();
            }

            return Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>>(snapshot);
        }

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
            string treeId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_entries.TryGetValue(treeId, out var entry) ? entry.Clone() : null);

        public Task WriteEntryAsync(
            string treeId,
            string replicaId,
            LatticeReplicationConfigEntry entry,
            CancellationToken cancellationToken = default)
        {
            WriteCount++;
            Merge(treeId, entry);
            return Task.CompletedTask;
        }

        private void Merge(string treeId, LatticeReplicationConfigEntry entry)
        {
            if (_entries.TryGetValue(treeId, out var existing))
            {
                existing.MergeFrom(entry);
            }
            else
            {
                _entries[treeId] = entry.Clone();
            }
        }
    }

    /// <summary>
    /// A test-local stand-in for the production <c>LatticeReplicationControl</c>
    /// facade (which lives in <c>Orleans.Lattice.Api.Replication</c> and is
    /// unit-tested there). It reproduces the facade's security contract - resolve
    /// the caller, authorize the whole tree for the dedicated
    /// <see cref="LatticeOperation.Replication"/> capability through the public
    /// <see cref="ILatticeAccessGate"/> seam <i>before</i> delegating to the
    /// engine authority, and scope <see cref="GetConfigAsync"/> to the trees the
    /// caller may manage - so this suite can prove the fail-closed composition
    /// end to end against the real engine using only reachable, public seams.
    /// </summary>
    private sealed class E2EReplicationControl(ILatticeReplicationConfigAuthority authority, ILatticeAccessGate gate)
    {
        public async Task<LatticeReplicationEnableResult> EnableAsync(
            string treeId, LatticeMergeMode mode, LatticeSubject subject)
        {
            await AuthorizeWholeTreeAsync(treeId, subject);
            return await authority.EnableReplicationAsync(treeId, mode);
        }

        public async Task<LatticeReplicationDisableResult> DisableAsync(string treeId, LatticeSubject subject)
        {
            await AuthorizeWholeTreeAsync(treeId, subject);
            return await authority.DisableReplicationAsync(treeId);
        }

        public async Task<IReadOnlyList<LatticeReplicationTreeStatus>> GetConfigAsync(LatticeSubject subject)
        {
            var statuses = await authority.GetAllTreeStatusesAsync();
            var visible = new List<LatticeReplicationTreeStatus>(statuses.Count);
            foreach (var status in statuses.Values)
            {
                if (await IsAuthorizedAsync(status.TreeId, subject))
                {
                    visible.Add(status);
                }
            }

            return visible;
        }

        private async ValueTask AuthorizeWholeTreeAsync(string treeId, LatticeSubject subject)
        {
            var request = new LatticeAccessRequest(treeId, LatticeOperation.Replication, subject);
            var decision = await gate.AuthorizeAsync(in request, CancellationToken.None);

            // Fail-closed exactly as the whole-tree enforcement primitive does: a
            // deny throws, and a partial (filtered) allow cannot narrow a
            // whole-tree control operation, so it is also refused.
            if (!decision.Allowed || decision.KeyFilter is not null)
            {
                throw new LatticeAuthorizationDeniedException(
                    treeId, LatticeOperation.Replication, subject.SubjectId,
                    decision.Reason ?? "Denied by access gate.");
            }
        }

        private async ValueTask<bool> IsAuthorizedAsync(string treeId, LatticeSubject subject)
        {
            try
            {
                await AuthorizeWholeTreeAsync(treeId, subject);
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }

    /// <summary>An <see cref="ILatticeAccessGate"/> that authorizes every request.</summary>
    private sealed class AllowAllGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    /// <summary>Denies anonymous callers, allowing every named subject (default-deny posture).</summary>
    private sealed class DenyAnonymousGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            request.Subject.IsAnonymous
                ? new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("anonymous"))
                : new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
    }

    /// <summary>Allows only the named trees, denying all others (permission-scoped discovery).</summary>
    private sealed class TreeScopedGate(params string[] allowed) : ILatticeAccessGate
    {
        private readonly HashSet<string> _allowed = new(allowed, StringComparer.Ordinal);

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            _allowed.Contains(request.TreeId)
                ? new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow())
                : new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("not in scope"));
    }
}
