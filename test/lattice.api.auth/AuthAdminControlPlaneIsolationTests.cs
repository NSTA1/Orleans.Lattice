using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Regression coverage for the admin control-plane isolation boundary (issue
/// #1103, finding A2). No rule can be scoped at the reserved authorization
/// namespace (<c>sys-auth-*</c>), so no rule ever matches the policy tree an admin
/// request targets. Before the fix, under the permissive data-plane default
/// effect (<see cref="LatticeEffect.Allow"/>) the unmatched admin request
/// inherited Allow, so any caller - including an anonymous one - passed
/// authorization and could rewrite membership and policy: a full control-plane
/// takeover. The fix forces every unmatched decision on the reserved namespace to
/// Deny, independent of the data-plane default, while leaving the break-glass
/// bootstrap administrator and the ordinary data plane untouched.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthAdminControlPlaneIsolationTests
{
    private AuthAdminControlPlaneClusterFixture _fixture = null!;

    private const string Intruder = "intruder";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthAdminControlPlaneClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    // Every mutating control-plane call the facade exposes, named for the assertion
    // messages. Each must be denied for a non-bootstrap or anonymous caller under
    // DefaultEffect=Allow.
    private (string Name, Func<Task> Invoke)[] MutatingAdminCalls() =>
    [
        ("PutRuleAsync", () => _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
            "escalation",
            LatticeSubjectSelector.User(Intruder),
            LatticeScope.Tree("victim-tree"),
            LatticeOperation.Read,
            LatticeEffect.Allow))),
        ("RemoveRuleAsync", () => _fixture.Admin.RemoveRuleAsync("victim-tree", "escalation")),
        ("UpsertGroupAsync", () => _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "cabal" })),
        ("AddMemberAsync", () => _fixture.Admin.AddMemberAsync("cabal", Intruder)),
        ("RemoveMemberAsync", () => _fixture.Admin.RemoveMemberAsync("cabal", Intruder)),
    ];

    [Test]
    public void Every_mutating_admin_call_is_denied_for_a_non_bootstrap_caller()
    {
        foreach (var (name, invoke) in MutatingAdminCalls())
        {
            using (AuthAdminControlPlaneClusterFixture.AsSubject(Intruder))
            {
                Assert.That(
                    async () => await invoke(),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                    $"{name} must be denied for a non-bootstrap caller even under DefaultEffect=Allow.");
            }
        }
    }

    [Test]
    public void Every_mutating_admin_call_is_denied_for_an_anonymous_caller()
    {
        // No ambient credential: the caller is anonymous. It must not inherit the
        // data-plane default-allow to seize the control plane.
        foreach (var (name, invoke) in MutatingAdminCalls())
        {
            Assert.That(
                async () => await invoke(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                $"{name} must be denied for an anonymous caller even under DefaultEffect=Allow.");
        }
    }

    [Test]
    public void Every_mutating_admin_call_succeeds_for_a_bootstrap_administrator()
    {
        // The break-glass root of trust must still be able to administer policy and
        // membership; control-plane isolation must not lock the operator out.
        foreach (var (name, invoke) in MutatingAdminCalls())
        {
            using (AuthAdminControlPlaneClusterFixture.AsSubject(AuthAdminControlPlaneClusterFixture.BootstrapAdmin))
            {
                Assert.That(
                    async () => await invoke(),
                    Throws.Nothing,
                    $"{name} must succeed for a bootstrap administrator.");
            }
        }
    }

    [Test]
    public async Task Data_plane_reads_and_writes_are_unaffected_by_control_plane_isolation()
    {
        // The forced-deny is scoped to the reserved authorization namespace only:
        // an ordinary data tree still enjoys the permissive DefaultEffect=Allow for
        // a non-bootstrap, non-anonymous caller.
        var tree = await _fixture.CreateTreeAsync("a2-data-plane");

        using (AuthAdminControlPlaneClusterFixture.AsSubject(Intruder))
        {
            await tree.SetAsync("k", new byte[] { 7 });
            var value = await tree.GetAsync("k");

            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.EqualTo(new byte[] { 7 }));
        }
    }

    [Test]
    public void Authoring_an_access_administration_delegation_rule_fails_closed_when_delegation_is_disabled()
    {
        // Delegation is off on this fixture. Even the bootstrap administrator (who
        // passes admin authorization) cannot author the delegation grant on the
        // reserved policy tree: the store rejects the reserved-namespace write
        // fail-closed.
        using (AuthAdminControlPlaneClusterFixture.AsSubject(AuthAdminControlPlaneClusterFixture.BootstrapAdmin))
        {
            Assert.That(
                async () => await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                    "delegate-off",
                    LatticeSubjectSelector.User("would-be-admin"),
                    LatticeScope.Tree("sys-auth-policy"),
                    LatticeOperation.Admin,
                    LatticeEffect.Allow)),
                Throws.ArgumentException,
                "no delegation rule may be authored on the reserved namespace while delegation is off");
        }
    }
}
