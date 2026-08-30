using System.Threading.Tasks;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Regression coverage for the cluster-wide scopeless capability contract
/// (issue #1795): a grant authored the way the API documents -
/// <see cref="LatticeScope.ClusterWide"/>, which targets the all-trees sentinel
/// <c>"*"</c> - must be the grant that authorizes
/// <see cref="LatticeOperation.Telemetry"/>, and it must do so without the
/// sentinel becoming a data-plane back door that inherits a permissive
/// <see cref="LatticeAuthOptions.DefaultEffect"/>.
/// </summary>
/// <remarks>
/// Both halves matter and pull in opposite directions, which is why they are
/// pinned together. Before the fix the authorizers asked about the reserved
/// policy tree while the documented helper authored <c>"*"</c>, so the grant was
/// silently inert; naively moving the request to <c>"*"</c> without governing
/// the sentinel as control plane would have handed the elevated all-tree
/// observability capability to any caller (including an anonymous one) on a host
/// running <c>DefaultEffect = Allow</c>. The gate therefore treats a request
/// <b>targeting</b> the sentinel as a scopeless capability request - a data-plane
/// read or write always names a real tree - and routes it through control-plane
/// isolation.
/// </remarks>
[TestFixture]
public sealed class ClusterWideTelemetryGrantTests
{
    private const string PolicyTree = "sys-auth-policy";
    private const string DataPlaneTree = "orders";

    private static LatticeAuthorizationRule Rule(
        LatticeScope scope,
        LatticeOperation operations,
        LatticeEffect effect = LatticeEffect.Allow,
        string subjectId = "auditor") =>
        new("cluster-telemetry", LatticeSubjectSelector.User(subjectId), scope, operations, effect);

    private static LatticeAccessRequest TelemetryRequest(string subjectId) =>
        new(LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, new LatticeSubject(subjectId));

    [Test]
    public async Task ClusterWide_telemetry_grant_is_honoured()
    {
        // The defect: this grant is exactly what LatticeScope.ClusterWide()'s own
        // documentation tells an operator to author for Telemetry, and it used to
        // authorize nothing because both authorizers asked about the reserved
        // policy tree instead.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        var harness = await AuthGateHarness.CreateAsync(
            options, Rule(LatticeScope.ClusterWide(), LatticeOperation.Telemetry));

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("auditor"));

        Assert.That(decision.Allowed, Is.True,
            "a cluster-wide Telemetry grant authored with LatticeScope.ClusterWide() must authorize "
            + "the cluster-wide Telemetry capability");
    }

    [Test]
    public async Task Unmatched_telemetry_on_the_sentinel_is_denied_under_default_effect_allow()
    {
        // The security property the previous reserved-tree scoping provided, which
        // the move to the sentinel must not give up: the elevated all-tree
        // observability capability is never inherited from the data-plane default.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };
        var harness = await AuthGateHarness.CreateAsync(options);

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("mallory"));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False,
                "an unmatched request on the cluster-wide sentinel must fail closed");
            Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
        });
    }

    [Test]
    public async Task Anonymous_telemetry_on_the_sentinel_is_denied_under_default_effect_allow()
    {
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };
        var harness = await AuthGateHarness.CreateAsync(options);

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest(
                LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, LatticeSubject.Anonymous));

        Assert.That(decision.Allowed, Is.False,
            "an anonymous caller must never inherit cluster telemetry from a permissive default");
    }

    [Test]
    public async Task A_data_plane_all_trees_grant_does_not_confer_telemetry()
    {
        // A broad wildcard Read grant is not authority to read cluster telemetry:
        // the capability bit is distinct, so the operation must still match.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Allow,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options, Rule(LatticeScope.ClusterWide(), LatticeOperation.Read));

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("auditor"));

        Assert.That(decision.Allowed, Is.False,
            "an all-trees data-plane Read grant must not confer the distinct Telemetry capability");
    }

    [Test]
    public async Task A_cluster_wide_telemetry_grant_does_not_reach_the_reserved_namespace()
    {
        // The sentinel must not become a route into the control plane: a caller
        // holding cluster-wide Telemetry gains nothing on the policy tree.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Allow,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options, Rule(LatticeScope.ClusterWide(), LatticeOperation.Telemetry));

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest(PolicyTree, LatticeOperation.Admin, new LatticeSubject("auditor")));

        Assert.That(decision.Allowed, Is.False,
            "the reserved namespace is still excluded from the all-trees tier");
    }

    [Test]
    public async Task Governing_the_sentinel_leaves_ordinary_data_plane_requests_unchanged()
    {
        // Guard against over-reach: only a request whose target IS the sentinel is
        // treated as control plane. A named tree still inherits the data-plane
        // default effect exactly as before.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };
        var harness = await AuthGateHarness.CreateAsync(options);

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest(DataPlaneTree, LatticeOperation.Read, new LatticeSubject("alice"), "k"));

        Assert.That(decision.Allowed, Is.True,
            "a data-plane request on a named tree is unaffected by the sentinel's control-plane isolation");
    }

    [Test]
    public async Task HasAnyGrantAsync_on_the_sentinel_mirrors_the_enforcement_decision()
    {
        // The existence probe must not out-reach enforcement: with no rule
        // authored, a permissive default must not report a grant on the sentinel.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };
        var harness = await AuthGateHarness.CreateAsync(options);

        var granted = await harness.Gate.HasAnyGrantAsync(
            LatticeScope.ClusterWideTreeId, new LatticeSubject("mallory"), LatticeOperation.Telemetry);

        Assert.That(granted, Is.False,
            "an unmatched probe on the cluster-wide sentinel must mirror the fail-closed decision");
    }

    [Test]
    public async Task HasAnyGrantAsync_on_the_sentinel_reports_an_explicit_grant()
    {
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        var harness = await AuthGateHarness.CreateAsync(
            options, Rule(LatticeScope.ClusterWide(), LatticeOperation.Telemetry));

        var granted = await harness.Gate.HasAnyGrantAsync(
            LatticeScope.ClusterWideTreeId, new LatticeSubject("auditor"), LatticeOperation.Telemetry);

        Assert.That(granted, Is.True,
            "an explicitly granted cluster-wide capability must still be visible to the probe");
    }

    [Test]
    public async Task An_unrelated_key_scoped_rule_in_the_sentinel_bucket_does_not_deny_the_grant()
    {
        // A scopeless capability is not attached to a key, so a key- or
        // prefix-scoped rule that happens to sit in the "*" bucket must not decide
        // it. Without this, any such rule - authorable through the ordinary admin
        // surface, belonging to a different subject, and inert for the all-trees
        // tier - flips the bucket's HasPerKeyRules, turns the whole-scope
        // evaluation into a per-key Filtered decision whose winning match is
        // "unmatched", and bricks cluster telemetry for every caller.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide(), LatticeOperation.Telemetry),
            new LatticeAuthorizationRule(
                "unrelated-key-rule",
                LatticeSubjectSelector.User("someone-else"),
                LatticeScope.Key(LatticeScope.ClusterWideTreeId, "irrelevant"),
                LatticeOperation.Telemetry,
                LatticeEffect.Allow));

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("auditor"));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True,
                "an unrelated key-scoped rule in the sentinel bucket must not deny a whole-scope grant");
            Assert.That(decision.KeyFilter, Is.Null,
                "a scopeless capability must resolve to a definite verdict, never a per-key filter");
        });
    }

    [Test]
    public async Task An_unrelated_prefix_scoped_rule_in_the_sentinel_bucket_does_not_deny_the_grant()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide(), LatticeOperation.Telemetry),
            new LatticeAuthorizationRule(
                "unrelated-prefix-rule",
                LatticeSubjectSelector.User("someone-else"),
                LatticeScope.Prefix(LatticeScope.ClusterWideTreeId, "public/"),
                LatticeOperation.Read,
                LatticeEffect.Allow));

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("auditor"));

        Assert.That(decision.Allowed, Is.True,
            "an unrelated prefix-scoped rule in the sentinel bucket must not deny a whole-scope grant");
    }

    [Test]
    public async Task A_key_scoped_rule_in_the_sentinel_bucket_does_not_confer_the_capability()
    {
        // The mirror image: resolving the sentinel whole-scope must not let a
        // key-scoped rule grant a capability its holder was never given tree-wide.
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            new LatticeAuthorizationRule(
                "key-only-rule",
                LatticeSubjectSelector.User("auditor"),
                LatticeScope.Key(LatticeScope.ClusterWideTreeId, "irrelevant"),
                LatticeOperation.Telemetry,
                LatticeEffect.Allow));

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("auditor"));

        Assert.That(decision.Allowed, Is.False,
            "a key-scoped rule must not confer a scopeless cluster-wide capability");
    }

    [Test]
    public async Task A_bootstrap_administrator_still_reaches_cluster_telemetry()
    {
        // The break-glass root of trust is checked before control-plane isolation,
        // so it is unaffected.
        var options = new LatticeAuthOptions
        {
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);

        var decision = await harness.Gate.AuthorizeAsync(TelemetryRequest("root"));

        Assert.That(decision.Allowed, Is.True, "a bootstrap administrator is unconditionally allowed");
    }
}
