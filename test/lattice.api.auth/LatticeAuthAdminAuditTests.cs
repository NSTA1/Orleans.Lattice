using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Binds the authorization administration facade to the admin audit protocol
/// (#3638): a refused call is recorded as a denial before the refusal returns and never
/// writes an intent, an authorized mutation writes its intent before the store is
/// touched and finalizes after it, and a removal that found nothing reports no change.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeAuthAdminAuditTests
{
    private static (LatticeAuthAdmin Admin, RecordingTrail Trail, ILatticeAuthorizationPolicyStore Store) Create(bool allow)
    {
        var trail = new RecordingTrail();
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.When(s => s.PutRuleAsync(Arg.Any<LatticeAuthorizationRule>(), Arg.Any<CancellationToken>()))
            .Do(_ => trail.Calls.Add("store"));
        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions());

        var admin = new LatticeAuthAdmin(
            store,
            Substitute.For<ILatticeMembershipDirectory>(),
            new Gate(allow),
            new Anonymous(),
            new NullIdentityDirectory(),
            new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() },
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor,
            auditTrail: trail);
        return (admin, trail, store);
    }

    private static LatticeAuthorizationRule Rule() =>
        new("r-1", LatticeSubjectSelector.User("u-1"), LatticeScope.Tree("app"), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void PutRuleAsync_refused_records_a_denial_and_writes_no_intent()
    {
        var (admin, trail, store) = Create(allow: false);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => admin.PutRuleAsync(Rule()));

        Assert.Multiple(() =>
        {
            Assert.That(trail.Calls, Is.EqualTo(new[] { "denied:RuleUpsert:" + LatticeAdminAuditReasonCodes.DeniedNotAuthorized }));
            Assert.That(store.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public async Task PutRuleAsync_authorized_writes_the_intent_before_the_store_then_finalizes()
    {
        var (admin, trail, _) = Create(allow: true);

        await admin.PutRuleAsync(Rule());

        Assert.Multiple(() =>
        {
            Assert.That(trail.Calls, Is.EqualTo(new[] { "begin:RuleUpsert", "store", "complete:Committed" }));
            Assert.That(trail.Begun.Single().Anchor, Is.EqualTo(PolicyAuditAnchors.Rule("app", "r-1")));
            Assert.That(trail.Begun.Single().Detail!.Keys, Is.EquivalentTo(new[] { "effect", "operations", "scopeKind" }),
                "the detail carries the rule's shape, never its subject");
        });
    }

    [Test]
    public async Task RemoveRuleAsync_that_removed_nothing_reports_no_change()
    {
        var (admin, trail, store) = Create(allow: true);
        store.RemoveRuleAsync("app", "r-1", Arg.Any<CancellationToken>()).Returns(false);

        await admin.RemoveRuleAsync("app", "r-1");

        Assert.That(trail.Calls[^1], Is.EqualTo("complete:Failed"));
    }

    [Test]
    public async Task RemoveMemberAsync_completes_as_no_change_unless_the_store_reports_a_removal()
    {
        // The removal returns no store report through a replaced directory, so the call
        // must never be recorded Committed on its own say-so (F9 trace 2).
        var (admin, trail, _) = Create(allow: true);

        await admin.RemoveMemberAsync("g", "alice");
        await admin.RemoveGroupAsync("g");

        Assert.That(trail.Calls.Where(c => c.StartsWith("complete:", StringComparison.Ordinal)), Has.All.EqualTo("complete:Failed"));
    }

    [Test]
    public void ListRulesAsync_refused_records_a_read_denial()
    {
        var (admin, trail, _) = Create(allow: false);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => admin.ListRulesAsync(new AuthPageRequest()));

        Assert.That(trail.Calls, Is.EqualTo(new[] { "denied:Read:" + LatticeAdminAuditReasonCodes.DeniedNotAuthorized }));
    }

    private sealed class RecordingTrail : ILatticeAdminAuditTrail
    {
        public List<string> Calls { get; } = [];

        public List<LatticeAdminAuditRequest> Begun { get; } = [];

        public bool IsEnabled => true;

        public ValueTask RecordDeniedAsync(LatticeAdminAuditRequest request, string reasonCode, CancellationToken cancellationToken)
        {
            Calls.Add($"denied:{request.Action}:{reasonCode}");
            return ValueTask.CompletedTask;
        }

        public ValueTask<LatticeAdminAuditRecord> BeginAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken)
        {
            Calls.Add($"begin:{request.Action}");
            Begun.Add(request);
            return ValueTask.FromResult(new LatticeAdminAuditRecord
            {
                OperationId = "op",
                ActorSubjectId = "a",
                TargetScope = request.TargetScope,
            });
        }

        public ValueTask CompleteAsync(LatticeAdminAuditRecord intent, LatticeAdminAuditCompletion completion)
        {
            Calls.Add($"complete:{completion.Outcome}");
            return ValueTask.CompletedTask;
        }
    }

    private sealed class Gate(bool allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied"));
    }

    private sealed class Anonymous : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);
    }
}
