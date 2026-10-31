using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A rule's effect, subject kind and scope kind are enums the public constructors
/// never range-check, and Orleans deserialization and <c>with</c> expressions skip
/// the constructors' id and key checks too. The evaluator read an undefined subject
/// kind as a group and an undefined scope kind as the whole tree, while tenant
/// confinement tested only the defined kinds, so a malformed rule - from a client, a
/// newer build's replication, or a restore - could carry a tenant group's grant past
/// confinement or widen a key grant to a whole tree. The store now refuses such a rule
/// before any read or write, and the compiler drops one that reached the policy tree
/// another way.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AuthorizationRuleShapeTests
{
    private static LatticeAuthorizationRule GroupReadsPayroll() =>
        new("r", LatticeSubjectSelector.Group("admins"), LatticeScope.Tree("payroll"), LatticeOperation.Read, LatticeEffect.Allow);

    private static IEnumerable<TestCaseData> MalformedRules()
    {
        var rule = GroupReadsPayroll();
        yield return new TestCaseData(rule with { Effect = (LatticeEffect)99 }).SetName("an undefined effect");
        yield return new TestCaseData(
                rule with { Subject = LatticeSubjectSelector.Group("t/acme/ops") with { Kind = (LatticeSubjectSelectorKind)2 } })
            .SetName("an undefined subject kind naming a tenant group");
        yield return new TestCaseData(rule with { Scope = LatticeScope.Tree("payroll") with { Kind = (LatticeScopeKind)99 } })
            .SetName("an undefined scope kind");
        yield return new TestCaseData(rule with { Scope = LatticeScope.Key("payroll", "k") with { KeyOrPrefix = null } })
            .SetName("a key scope without a key");
        yield return new TestCaseData(rule with { Scope = LatticeScope.Tree("payroll") with { KeyOrPrefix = "public/" } })
            .SetName("a whole-tree scope carrying a prefix");
        yield return new TestCaseData(rule with { Subject = LatticeSubjectSelector.User("x") with { Id = string.Empty } })
            .SetName("an empty subject id");
    }

    [TestCaseSource(nameof(MalformedRules))]
    public void The_store_refuses_a_malformed_rule_before_writing_anything(LatticeAuthorizationRule rule)
    {
        var grains = Substitute.For<IGrainFactory>();
        var policy = Substitute.For<ILattice>();
        grains.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(policy);
        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions());
        var store = new LatticeAuthorizationPolicyStore(
            grains, new AuthInitializer(grains, Substitute.For<IServiceProvider>(), options), options);

        Assert.That(async () => await store.PutRuleAsync(rule), Throws.ArgumentException);
        Assert.That(policy.ReceivedCalls(), Is.Empty, "a refused rule issues no read or write");
    }

    [TestCaseSource(nameof(MalformedRules))]
    public async Task A_malformed_rule_that_reaches_the_snapshot_grants_nothing(LatticeAuthorizationRule rule)
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, rule);
        var member = new LatticeSubject("alice", new HashSet<string>(StringComparer.Ordinal) { "admins", "t/acme/ops" });

        var decision = await harness.Gate.AuthorizeAsync(new LatticeAccessRequest("payroll", LatticeOperation.Read, member, "k"));

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public async Task A_previously_persisted_separator_id_still_enforces_its_deny()
    {
        var rule = GroupReadsPayroll() with { RuleId = "legacy\u001frule", Effect = LatticeEffect.Deny };
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, rule);
        var member = new LatticeSubject("alice", new HashSet<string>(StringComparer.Ordinal) { "admins" });

        var decision = await harness.Gate.AuthorizeAsync(new LatticeAccessRequest("payroll", LatticeOperation.Read, member, "k"));

        Assert.That(decision.Allowed, Is.False, "new authoring validation must not retire an existing deny");
    }

    [Test]
    public async Task A_well_formed_rule_still_grants()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, GroupReadsPayroll());
        var member = new LatticeSubject("alice", new HashSet<string>(StringComparer.Ordinal) { "admins" });

        var decision = await harness.Gate.AuthorizeAsync(new LatticeAccessRequest("payroll", LatticeOperation.Read, member, "k"));

        Assert.That(decision.Allowed, Is.True);
    }
}
