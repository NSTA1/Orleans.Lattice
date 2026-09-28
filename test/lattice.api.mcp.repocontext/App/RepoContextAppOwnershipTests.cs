using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The RepoContext pilot under tree ownership: the <c>repo-context</c> app still installs and enables
/// over its pre-existing legacy trees, which it adopts and then owns exclusively. Runs the real
/// registry, ownership ledger and activation engine over in-memory stores.
/// </summary>
[TestFixture]
public sealed class RepoContextAppOwnershipTests
{
    private static readonly AppSlug Slug = AppSlug.Parse(RepoContextAppManifest.Slug);

    private MemoryRegistryStore _registryStore = null!;
    private MemoryLedgerStore _ledgerStore = null!;
    private AppTreeOwnershipLedger _ledger = null!;
    private AppRegistry _registry = null!;
    private AppActivationEngine _engine = null!;
    private MemoryPolicyStore _rules = null!;
    private AppManifest _manifest = null!;

    [SetUp]
    public void SetUp()
    {
        var loaded = RepoContextAppManifest.Load();
        Assert.That(loaded.IsValid, Is.True);
        _manifest = loaded.Manifest!;

        var options = new InImageAppSourceOptions();
        options.Registrations.Add(new InImageAppRegistration(Slug, typeof(RepoContextAppManifest).Assembly, RepoContextAppManifest.ResourceName));
        var source = new InImageAppSource(Options.Create(options));

        // Every adopted legacy tree already exists, as on a deployment that predates the app.
        var facts = Substitute.For<IAppTreeFacts>();
        facts.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        facts.GetDerivedFromAsync(Arg.Any<string>()).Returns(Task.FromResult<string?>(null));
        facts.ResolveAsync(Arg.Any<string>()).Returns(call => Task.FromResult(call.ArgAt<string>(0)));
        facts.GetAliasesTargetingAsync(Arg.Any<string>()).Returns(Task.FromResult<IReadOnlyList<string>>([]));

        _registryStore = new MemoryRegistryStore();
        _ledgerStore = new MemoryLedgerStore();
        _ledger = new AppTreeOwnershipLedger(_ledgerStore, facts, _registryStore);
        _registry = new AppRegistry(
            _registryStore,
            new AppInstallAuthorizer(new RepoContextAppGrantingGate()),
            Options.Create(new ClusterOptions { ClusterId = "pilot" }),
            _ledger,
            source);
        _rules = new MemoryPolicyStore();
        var status = Substitute.For<IAppActivationStatusStore>();
        status.GetAsync(default, default, default).ReturnsForAnyArgs(Task.FromResult<AppActivationStatus?>(null));
        _engine = new AppActivationEngine(
            _registry,
            source,
            status,
            Substitute.For<IAppTreeProvisioner>(),
            _ledger,
            NullLogger<AppActivationEngine>.Instance,
            _rules,
            new RepoContextAppMembershipContext());
    }

    private AppRegistryInstallRequest Consent()
    {
        var operations = LatticeOperation.None;
        foreach (var role in _manifest.Roles)
            operations |= role.Operations;
        return new AppRegistryInstallRequest
        {
            Identity = _manifest.Identity,
            Ceiling = new AppCapabilityCeiling
            {
                AllowedOperations = operations,
                ApprovedExceptionScopes = _manifest.Trees.Select(t => LatticeScope.Tree(t.AdoptedTreeId!)).ToArray(),
            },
            RoleBindings = [AppRoleBinding.Create("reader", "repo-readers")],
        };
    }

    [Test]
    public async Task The_pilot_installs_and_enables_adopting_its_legacy_trees()
    {
        AppRegistryTransitionResult installed;
        AppActivationOutcome enabled;
        using (LatticeSystemOrigin.Enter())
        {
            installed = await _registry.InstallAsync(Consent());
            enabled = await _engine.ExecuteAsync(AppActivationOperation.Enable, TenantId.Default, Slug, CancellationToken.None);
        }

        Assert.That(installed.Succeeded, Is.True, installed.Message);
        Assert.That(enabled.Succeeded, Is.True, () => string.Join("; ", enabled.Diagnostics.Select(d => d.Message)));
        Assert.That(enabled.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(_rules.Rules, Is.Not.Empty);

        var adopted = _manifest.Trees.Select(t => t.AdoptedTreeId!).ToArray();
        Assert.That(_ledgerStore.Keys, Is.EquivalentTo(adopted));
        foreach (var tree in adopted)
        {
            var claim = (await _ledgerStore.GetAsync(tree, CancellationToken.None)).Claim!;
            Assert.That((claim.Slug, claim.Kind, claim.Released), Is.EqualTo((Slug, AppTreeClaimKind.Adopted, false)), tree);
        }
    }

    [Test]
    public async Task Another_install_cannot_adopt_a_tree_the_pilot_owns()
    {
        using (LatticeSystemOrigin.Enter())
        {
            await _registry.InstallAsync(Consent());
        }

        var tree = _manifest.Trees[0].AdoptedTreeId!;
        var conflict = await _ledger.ClaimAsync(
            new AppTreeOwner(TenantId.Default, AppSlug.Parse("rival"), "first-party"),
            1,
            [new AppTreeClaimPlan(tree, "copy", AppTreeClaimKind.Adopted)],
            null,
            CancellationToken.None);

        Assert.That(conflict!.OwningApp, Is.EqualTo(Slug));
    }

    private sealed class MemoryRegistryStore : IAppRegistryStore
    {
        private readonly SortedDictionary<string, (AppRegistryRecord Record, HybridLogicalClock Version)> _entries = new(StringComparer.Ordinal);
        private long _ticks;

        public Task<AppRegistryStoreRead> GetAsync(string key, CancellationToken cancellationToken) =>
            Task.FromResult(_entries.TryGetValue(key, out var e) ? new AppRegistryStoreRead(e.Record, e.Version) : new AppRegistryStoreRead(null, HybridLogicalClock.Zero));

        public Task<bool> TrySetAsync(string key, AppRegistryRecord record, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
        {
            var current = _entries.TryGetValue(key, out var e) ? e.Version : HybridLogicalClock.Zero;
            if (!current.Equals(expectedVersion))
                return Task.FromResult(false);
            _entries[key] = (record, new HybridLogicalClock { WallClockTicks = ++_ticks });
            return Task.FromResult(true);
        }

        public async IAsyncEnumerable<AppRegistryRecord> ScanAsync(string? startInclusive, string? endExclusive, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var entry in _entries.Values.ToArray())
                yield return entry.Record;
            await Task.CompletedTask;
        }
    }

    private sealed class MemoryLedgerStore : IAppTreeLedgerStore
    {
        private readonly SortedDictionary<string, (AppTreeClaim Claim, HybridLogicalClock Version)> _entries = new(StringComparer.Ordinal);
        private long _ticks;

        public IReadOnlyList<string> Keys => _entries.Keys.ToArray();

        public Task<AppTreeLedgerRead> GetAsync(string treeId, CancellationToken cancellationToken) =>
            Task.FromResult(_entries.TryGetValue(treeId, out var e) ? new AppTreeLedgerRead(e.Claim, e.Version) : new AppTreeLedgerRead(null, HybridLogicalClock.Zero));

        public Task<bool> TrySetAsync(string treeId, AppTreeClaim claim, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
        {
            var current = _entries.TryGetValue(treeId, out var e) ? e.Version : HybridLogicalClock.Zero;
            if (!current.Equals(expectedVersion))
                return Task.FromResult(false);
            _entries[treeId] = (claim, new HybridLogicalClock { WallClockTicks = ++_ticks });
            return Task.FromResult(true);
        }

        public async IAsyncEnumerable<KeyValuePair<string, AppTreeClaim>> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var (key, entry) in _entries.ToArray())
                yield return new(key, entry.Claim);
            await Task.CompletedTask;
        }
    }

    private sealed class MemoryPolicyStore : ILatticeAuthorizationPolicyStore
    {
        private readonly Dictionary<(string Tree, string Id), LatticeAuthorizationRule> _rules = new();

        public IReadOnlyCollection<LatticeAuthorizationRule> Rules => _rules.Values;

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            _rules[(rule.Scope.TreeId, rule.RuleId)] = rule;
            return Task.CompletedTask;
        }

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rules.TryGetValue((treeId, ruleId), out var rule) ? rule : null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rules.Remove((treeId, ruleId)));

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in _rules.Values.Where(r => r.Scope.TreeId == treeId).ToArray())
                yield return rule;
            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in _rules.Values.ToArray())
                yield return rule;
            await Task.CompletedTask;
        }
    }
}
