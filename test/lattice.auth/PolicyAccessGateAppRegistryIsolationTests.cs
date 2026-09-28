using System.Threading.Tasks;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Control-plane read isolation of the app-registry system-data namespace
/// (<c>sys-app-*</c>). The registry holds every installed app's capability ceiling,
/// consent record, and role-to-group bindings, so - exactly as for the tenant registry -
/// a data-plane read grant, including the cluster-wide all-trees (<c>Tree:*</c>) wildcard
/// and a data-plane default effect of allow, must not expose it, while an explicit rule
/// an operator deliberately scoped at the registry tree is honoured. Direct in-process
/// tests of the real <see cref="PolicyAccessGate"/> over an in-memory policy store.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class PolicyAccessGateAppRegistryIsolationTests
{
    private const string AppRegistryTree = "sys-app-registry";
    private const string AppRegistryHistoryTree = "sys-app-registry-history";
    private const string AppTreeLedgerTree = "sys-app-trees";

    private static LatticeAuthorizationRule Rule(
        LatticeScope scope,
        LatticeOperation operations,
        string subjectId) =>
        new("r", LatticeSubjectSelector.User(subjectId), scope, operations, LatticeEffect.Allow);

    private static LatticeAuthOptions WildcardOptions() => new()
    {
        DefaultEffect = LatticeEffect.Deny,
        AllTreesGrantsEnabled = true,
    };

    [TestCase(AppRegistryTree)]
    [TestCase(AppRegistryHistoryTree)]
    [TestCase(AppTreeLedgerTree)]
    public async Task AuthorizeAsync_app_registry_read_unmatched_is_denied_even_under_default_allow(string treeId)
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(treeId, LatticeOperation.Read, new LatticeSubject("mallory"), "default/notes");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "the whole sys-app-* prefix is control-plane isolated");
        Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
    }

    [Test]
    public async Task AuthorizeAsync_app_registry_scan_unmatched_is_denied_even_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(AppRegistryTree, LatticeOperation.RangeRead, new LatticeSubject("mallory"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "a whole-registry scan is the exfiltration shape and must fail closed");
    }

    [TestCase(LatticeOperation.Read)]
    [TestCase(LatticeOperation.RangeRead)]
    public async Task AuthorizeAsync_app_registry_denied_despite_cluster_wide_wildcard_read_grant(LatticeOperation operation)
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeAuthOperations.All, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest(AppRegistryTree, operation, new LatticeSubject("mallory"), "default/notes");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "a Tree:* wildcard never reaches the app registry");
    }

    [Test]
    public async Task AuthorizeAsync_the_same_wildcard_still_reaches_an_app_data_tree()
    {
        // Over-exclusion guard: the wildcard must keep working for ordinary trees,
        // including the app-owned a/{app}/{tree} namespace the registry describes.
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest("a/notes/pages", LatticeOperation.Read, new LatticeSubject("mallory"), "k");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task AuthorizeAsync_app_registry_honours_an_explicit_matched_allow()
    {
        var explicitAllow = Rule(LatticeScope.Tree(AppRegistryTree), LatticeOperation.Read, "alice");
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);
        var request = new LatticeAccessRequest(AppRegistryTree, LatticeOperation.Read, new LatticeSubject("alice"), "default/notes");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "an operator-scoped rule on the registry tree is the deliberate escape hatch");
    }

    [Test]
    public async Task AuthorizeAsync_bootstrap_administrator_may_read_the_app_registry()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);
        var request = new LatticeAccessRequest(AppRegistryTree, LatticeOperation.Read, new LatticeSubject("root"), "default/notes");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task HasAnyGrantAsync_app_registry_unmatched_is_false_even_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var granted = await harness.Gate.HasAnyGrantAsync(AppRegistryTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False, "an ordinary caller cannot even learn the app registry exists");
    }

    [Test]
    public async Task HasAnyGrantAsync_app_registry_not_surfaced_by_cluster_wide_wildcard_grant()
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(AppRegistryTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False);
    }

    [Test]
    public async Task HasAnyGrantAsync_app_registry_matched_allow_is_true()
    {
        var explicitAllow = Rule(LatticeScope.Tree(AppRegistryTree), LatticeOperation.Read, "alice");
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);

        var granted = await harness.Gate.HasAnyGrantAsync(AppRegistryTree, new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.True);
    }

    [Test]
    public async Task AuthorizeAsync_tree_ownership_ledger_scan_unmatched_is_denied_even_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(AppTreeLedgerTree, LatticeOperation.RangeRead, new LatticeSubject("mallory"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "the ownership ledger names every app-owned tree and must not be enumerable");
    }

    [TestCase(LatticeOperation.Read)]
    [TestCase(LatticeOperation.RangeRead)]
    public async Task AuthorizeAsync_tree_ownership_ledger_denied_despite_cluster_wide_wildcard_read_grant(LatticeOperation operation)
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeAuthOperations.All, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest(AppTreeLedgerTree, operation, new LatticeSubject("mallory"), "a/crm/contacts");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "a Tree:* wildcard never reaches the tree ownership ledger");
    }

    [Test]
    public async Task HasAnyGrantAsync_tree_ownership_ledger_not_surfaced_by_cluster_wide_wildcard_grant()
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(AppTreeLedgerTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False);
    }

    [Test]
    public async Task Tenant_registry_isolation_is_unchanged_by_the_combined_predicate()
    {
        // Regression guard for the predicate swap: sys-tenant-* stays isolated.
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest("sys-tenant-registry", LatticeOperation.Read, new LatticeSubject("mallory"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False);
    }
}
