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
}
