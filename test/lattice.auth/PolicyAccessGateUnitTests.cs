using System.Threading;
using System.Threading.Tasks;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Direct in-process unit tests for <see cref="PolicyAccessGate"/> - the real
/// enforcement gate - built over an in-memory policy store (no cluster). Covers the
/// bootstrap root-of-trust bypass, control-plane isolation of the reserved
/// <c>sys-auth-*</c> namespace (an unmatched reserved request fails closed even
/// under a data-plane default effect of allow, while an explicit matched delegation
/// allow is honoured), and every branch of the <see cref="ILatticeReadGrantProbe"/>
/// existence-hiding probe.
/// </summary>
[TestFixture]
public sealed class PolicyAccessGateUnitTests
{
    private const string PolicyTree = "sys-auth-policy";

    private static LatticeAuthorizationRule Rule(
        LatticeScope scope,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow,
        string subjectId = "alice") =>
        new("r", LatticeSubjectSelector.User(subjectId), scope, operations, effect);

    // ---- Bootstrap root-of-trust ----------------------------------------

    [Test]
    public async Task AuthorizeAsync_bootstrap_administrator_is_allowed_on_any_tree()
    {
        var options = new LatticeAuthOptions { BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" } };
        var harness = await AuthGateHarness.CreateAsync(options);
        var request = new LatticeAccessRequest("app", LatticeOperation.Write, new LatticeSubject("root"), "k");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "a bootstrap administrator is unconditionally allowed");
    }

    // ---- Control-plane isolation (reserved namespace) --------------------

    [Test]
    public async Task AuthorizeAsync_reserved_namespace_unmatched_non_bootstrap_is_denied_even_under_default_allow()
    {
        // Data-plane default effect is Allow, yet the reserved namespace must still
        // fail closed for an unmatched non-bootstrap caller.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };
        var harness = await AuthGateHarness.CreateAsync(options);
        var request = new LatticeAccessRequest(PolicyTree, LatticeOperation.Admin, new LatticeSubject("mallory"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "control-plane isolation denies an unmatched reserved request");
        Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
    }

    [Test]
    public async Task AuthorizeAsync_reserved_namespace_matched_delegation_allow_is_honoured()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AccessAdministrationDelegationEnabled = true,
        };
        var delegation = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(options, delegation);
        var request = new LatticeAccessRequest(PolicyTree, LatticeOperation.Admin, new LatticeSubject("alice"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "an explicit whole-tree Admin allow delegates control-plane access");
    }

    // ---- HasAnyGrantAsync (existence-hiding probe) -----------------------

    [Test]
    public void HasAnyGrantAsync_empty_tree_id_throws()
    {
        var harness = AuthGateHarness.CreateAsync(new LatticeAuthOptions()).GetAwaiter().GetResult();

        Assert.That(
            async () => await harness.Gate.HasAnyGrantAsync(string.Empty, new LatticeSubject("alice"), LatticeOperation.Read),
            Throws.ArgumentException);
    }

    [Test]
    public async Task HasAnyGrantAsync_anonymous_subject_is_false()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var granted = await harness.Gate.HasAnyGrantAsync("app", LatticeSubject.Anonymous, LatticeOperation.Read);

        Assert.That(granted, Is.False, "an anonymous caller can never read any key");
    }

    [Test]
    public async Task HasAnyGrantAsync_bootstrap_administrator_is_true()
    {
        var options = new LatticeAuthOptions { BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" } };
        var harness = await AuthGateHarness.CreateAsync(options);

        var granted = await harness.Gate.HasAnyGrantAsync("app", new LatticeSubject("root"), LatticeOperation.Read);

        Assert.That(granted, Is.True, "a bootstrap administrator can read every tree");
    }

    [Test]
    public async Task HasAnyGrantAsync_reserved_namespace_matched_allow_is_true()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AccessAdministrationDelegationEnabled = true,
        };
        var delegation = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(options, delegation);

        var granted = await harness.Gate.HasAnyGrantAsync(PolicyTree, new LatticeSubject("alice"), LatticeOperation.Admin);

        Assert.That(granted, Is.True, "a delegated admin can see the reserved namespace");
    }

    [Test]
    public async Task HasAnyGrantAsync_reserved_namespace_unmatched_is_false_even_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var granted = await harness.Gate.HasAnyGrantAsync(PolicyTree, new LatticeSubject("mallory"), LatticeOperation.Admin);

        Assert.That(granted, Is.False, "the reserved namespace is hidden from a caller without an explicit grant");
    }

    [Test]
    public async Task HasAnyGrantAsync_ordinary_tree_with_a_grant_is_true()
    {
        var grant = Rule(LatticeScope.Tree("app"), LatticeOperation.Read, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, grant);

        var granted = await harness.Gate.HasAnyGrantAsync("app", new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.True, "an ordinary-tree grant delegates to the decision engine and resolves to allow");
    }

    [Test]
    public async Task HasAnyGrantAsync_ordinary_tree_without_a_grant_is_false()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny });

        var granted = await harness.Gate.HasAnyGrantAsync("app", new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.False, "no grant and a deny default hides the tree");
    }

    // ---- Control-plane isolation: tenant registry (issue #1671) ----------
    //
    // The tenant-registry system-data namespace (sys-tenant-*) holds the
    // cross-tenant registry - every tenant's admin subjects, quotas, placement,
    // and grants. It must be governed with control-plane read isolation, so a
    // broad data-plane read grant (including a cluster-wide all-trees wildcard)
    // can never scan it and exfiltrate one tenant's metadata to another.

    private const string RegistryTree = "sys-tenant-registry";
    private const string UsageTree = "sys-tenant-usage";
    private const string OverageTree = "sys-tenant-overage";

    [Test]
    public async Task AuthorizeAsync_tenant_registry_read_unmatched_non_bootstrap_is_denied_even_under_default_allow()
    {
        // The core leak: a data-plane default effect of Allow (or any broad grant)
        // must NOT let an ordinary caller read the tenant registry.
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(RegistryTree, LatticeOperation.Read, new LatticeSubject("mallory"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "control-plane isolation denies an unmatched tenant-registry read");
        Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
    }

    [Test]
    public async Task AuthorizeAsync_tenant_registry_scan_unmatched_non_bootstrap_is_denied_even_under_default_allow()
    {
        // A whole-tree scan (RangeRead, key == null) is the exact shape the fuzz
        // test used to exfiltrate the registry; it must fail closed too.
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(RegistryTree, LatticeOperation.RangeRead, new LatticeSubject("mallory"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "control-plane isolation denies an unmatched tenant-registry scan");
        Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
    }

    [TestCase(RegistryTree)]
    [TestCase(UsageTree)]
    [TestCase(OverageTree)]
    public async Task AuthorizeAsync_every_tenant_registry_tree_read_is_denied_under_default_allow(string treeId)
    {
        // The whole sys-tenant-* prefix is isolated, not just the registry tree:
        // the usage and overage stores carry per-tenant accounting that is equally
        // confidential.
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(treeId, LatticeOperation.Read, new LatticeSubject("mallory"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, $"'{treeId}' is in the isolated tenant-registry namespace");
    }

    [Test]
    public async Task AuthorizeAsync_tenant_registry_read_denied_despite_cluster_wide_wildcard_read_grant()
    {
        // The critical wildcard-defeat test: an all-trees (Tree:*) Read grant to the
        // subject must NOT reach the tenant registry, because the evaluator excludes
        // the namespace from the all-trees tier. Without that exclusion the gate's
        // "matched Allow" escape hatch would honour the wildcard and the fix would be
        // defeated.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow, "mallory");
        var harness = await AuthGateHarness.CreateAsync(options, wildcard);
        var request = new LatticeAccessRequest(RegistryTree, LatticeOperation.Read, new LatticeSubject("mallory"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "a cluster-wide wildcard read grant never reaches the tenant registry");
    }

    [Test]
    public async Task AuthorizeAsync_ordinary_tree_still_readable_via_cluster_wide_wildcard_read_grant()
    {
        // Regression guard against over-exclusion: the all-trees tier must keep
        // working for genuine application trees. Same options and wildcard grant as
        // the test above, but a normal tree - here the wildcard MUST allow.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow, "mallory");
        var harness = await AuthGateHarness.CreateAsync(options, wildcard);
        var request = new LatticeAccessRequest("app", LatticeOperation.Read, new LatticeSubject("mallory"), "k");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "an application tree is still reachable through the all-trees tier");
    }

    [Test]
    public async Task AuthorizeAsync_tenant_registry_read_honours_an_explicit_matched_allow()
    {
        // The deliberate escape hatch, consistent with the control-plane model: an
        // operator can author an explicit rule scoped exactly at the registry tree,
        // and a matched Allow is honoured even though the default effect is Deny.
        var explicitAllow = Rule(LatticeScope.Tree(RegistryTree), LatticeOperation.Read, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);
        var request = new LatticeAccessRequest(RegistryTree, LatticeOperation.Read, new LatticeSubject("alice"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "an explicit matched allow on the registry tree is honoured");
    }

    [Test]
    public async Task AuthorizeAsync_bootstrap_administrator_may_read_the_tenant_registry()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);
        var request = new LatticeAccessRequest(RegistryTree, LatticeOperation.Read, new LatticeSubject("root"), "acme");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "the bootstrap root-of-trust can read the registry");
    }

    [Test]
    public async Task HasAnyGrantAsync_tenant_registry_unmatched_is_false_even_under_default_allow()
    {
        // Existence-hiding: an ordinary caller cannot even learn the registry exists.
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var granted = await harness.Gate.HasAnyGrantAsync(RegistryTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False, "the tenant registry is hidden from a caller without an explicit grant");
    }

    [Test]
    public async Task HasAnyGrantAsync_tenant_registry_not_surfaced_by_cluster_wide_wildcard_grant()
    {
        // The existence-hiding mirror of the wildcard-defeat test: a wildcard grant
        // must not surface the registry in listings either.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow, "mallory");
        var harness = await AuthGateHarness.CreateAsync(options, wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(RegistryTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False, "a cluster-wide wildcard grant never surfaces the tenant registry");
    }

    [Test]
    public async Task HasAnyGrantAsync_tenant_registry_matched_allow_is_true()
    {
        var explicitAllow = Rule(LatticeScope.Tree(RegistryTree), LatticeOperation.Read, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);

        var granted = await harness.Gate.HasAnyGrantAsync(RegistryTree, new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.True, "an explicit registry grant surfaces the tree to its holder");
    }

    // ---- Existence probe composes tenant isolation (issue #1678) ---------
    //
    // HasAnyGrantAsync is the existence-hiding mirror of the enforcement path, so
    // it must never out-reach it. It previously delegated straight to the decision
    // engine with no tenant-enforcer consultation, so a caller holding a broad
    // cluster-wide grant (or running under DefaultEffect=Allow) satisfied the probe
    // for ANOTHER tenant's t/{tenant}/... tree and learned it exists - the whole
    // tenant roster and every tenant's tree names - while the very same subject was
    // denied the moment it tried to read that tree.

    private const string ForeignTenantTree = "t/victim/orders";

    /// <summary>A tenant enforcer that denies exactly one tree and allows the rest.</summary>
    private sealed class DenyingTenantEnforcer(string deniedTreeId) : ITenantGateEnforcer
    {
        public bool IsActive => true;

        public LatticeAccessDecision Enforce(in LatticeAccessRequest request) =>
            string.Equals(request.TreeId, deniedTreeId, StringComparison.Ordinal)
                ? LatticeAccessDecision.Deny("tenant isolation denied the request")
                : LatticeAccessDecision.Allow();
    }

    [Test]
    public async Task HasAnyGrantAsync_foreign_tenant_tree_is_hidden_despite_a_cluster_wide_grant()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.RangeRead, LatticeEffect.Allow, "mallory");
        var harness = await AuthGateHarness.CreateAsync(
            options, new DenyingTenantEnforcer(ForeignTenantTree), wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(
            ForeignTenantTree, new LatticeSubject("mallory"), LatticeOperation.RangeRead);

        Assert.That(granted, Is.False,
            "a broad grant must not let an existence probe out-reach the enforcement decision");
    }

    [Test]
    public async Task HasAnyGrantAsync_foreign_tenant_tree_is_hidden_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow },
            new DenyingTenantEnforcer(ForeignTenantTree));

        var granted = await harness.Gate.HasAnyGrantAsync(
            ForeignTenantTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False,
            "the data-plane default effect must never surface another tenant's tree");
    }

    [Test]
    public async Task HasAnyGrantAsync_own_tenant_tree_is_still_visible()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(
            options, new DenyingTenantEnforcer(ForeignTenantTree), wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(
            "t/alice-co/orders", new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.True, "the caller's own tenant trees stay visible");
    }

    [Test]
    public async Task HasAnyGrantAsync_without_tenancy_is_unchanged()
    {
        // The no-tenancy path must be byte-for-byte identical: the null enforcer
        // reports IsActive false and the probe short-circuits after one bool read.
        var grant = Rule(LatticeScope.Tree("app"), LatticeOperation.Read, LatticeEffect.Allow, "alice");
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, tenantEnforcer: null, grant);

        var granted = await harness.Gate.HasAnyGrantAsync("app", new LatticeSubject("alice"), LatticeOperation.Read);

        Assert.That(granted, Is.True);
    }

    [Test]
    public async Task HasAnyGrantAsync_denied_by_policy_never_consults_the_tenant_enforcer()
    {
        var enforcer = new CountingTenantEnforcer();
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, enforcer);

        var granted = await harness.Gate.HasAnyGrantAsync(
            ForeignTenantTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(granted, Is.False);
            Assert.That(enforcer.Calls, Is.Zero,
                "the enforcer stays off the deny fast path, exactly as on the enforcement path");
        });
    }

    private sealed class CountingTenantEnforcer : ITenantGateEnforcer
    {
        public int Calls { get; private set; }

        public bool IsActive => true;

        public LatticeAccessDecision Enforce(in LatticeAccessRequest request)
        {
            Calls++;
            return LatticeAccessDecision.Allow();
        }
    }
}
