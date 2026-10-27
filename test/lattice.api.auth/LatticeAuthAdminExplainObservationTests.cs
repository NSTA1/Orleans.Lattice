using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// <see cref="LatticeAuthAdmin.ExplainAsync"/> runs the explained subject's request
/// through the same gate the data plane uses, so the gate's observer counted and
/// audited it as that subject's own access attempt: an administrator could write a
/// denial into the audit trail naming any subject. The explained evaluation now runs
/// inside an <see cref="AuthExplainScope"/>, and the caller's own administrator check
/// stays outside it, so that real decision is still observed.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminExplainObservationTests
{
    [Test]
    public async Task ExplainAsync_marks_only_the_explained_evaluation_as_an_explain()
    {
        var gate = new ScopeRecordingGate();
        var admin = CreateAdmin(gate);

        _ = await admin.ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("orders"));

        var explained = gate.Calls.Where(c => c.SubjectId == "alice").ToList();
        var others = gate.Calls.Where(c => c.SubjectId != "alice").ToList();
        Assert.That(explained, Has.Count.EqualTo(1));
        Assert.That(explained[0].InExplainScope, Is.True, "the explained subject's evaluation is not its access attempt");
        Assert.That(others, Is.Not.Empty, "the caller's own administrator check reaches the gate");
        Assert.That(others.All(c => !c.InExplainScope), Is.True, "the caller's own check is a real decision");
        Assert.That(AuthExplainScope.IsActive, Is.False, "the scope ends when the explain returns");
    }

    private static LatticeAuthAdmin CreateAdmin(ILatticeAccessGate gate)
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.ListRulesForTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => NoRules());
        store.ListRulesAsync(Arg.Any<CancellationToken>())
            .Returns(_ => NoRules());

        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.GroupsOfAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyCollection<string>>([]));

        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions());

        return new LatticeAuthAdmin(
            store,
            directory,
            gate,
            new AnonymousMembershipContext(),
            Substitute.For<ILatticeIdentityDirectory>(),
            [new AnonymousCredentialAuthenticator()],
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor);
    }

    private static async IAsyncEnumerable<LatticeAuthorizationRule> NoRules()
    {
        await Task.Yield();
        yield break;
    }

    private sealed class AnonymousMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }

    private sealed class ScopeRecordingGate : ILatticeAccessGate
    {
        private readonly List<(string SubjectId, bool InExplainScope)> _calls = [];

        public IReadOnlyList<(string SubjectId, bool InExplainScope)> Calls
        {
            get
            {
                lock (_calls)
                {
                    return _calls.ToArray();
                }
            }
        }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            lock (_calls)
            {
                _calls.Add((request.Subject.SubjectId, AuthExplainScope.IsActive));
            }

            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
        }
    }
}
