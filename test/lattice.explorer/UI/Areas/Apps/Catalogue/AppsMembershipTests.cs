using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The caller's groups are read through the auth facade under their own credential, for
/// the subject a Basic sign-in names, and are unknown - never guessed - otherwise.
/// </summary>
[TestFixture]
public sealed class AppsMembershipTests
{
    [Test]
    public async Task A_basic_sign_in_reads_the_signed_in_subjects_transitive_groups()
    {
        var (membership, auth, session) = Create();
        session.SignIn("explorer-admin");
        auth.ListSubjectGroupsAsync("explorer-admin", Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<string>>(["admins", "platform"]));

        var groups = await membership.ReadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(groups.IsKnown, Is.True);
            Assert.That(groups.SubjectId, Is.EqualTo("explorer-admin"));
            Assert.That(groups.Groups, Is.EquivalentTo(new[] { "admins", "platform" }));
        });
    }

    [Test]
    public async Task A_refused_read_is_unknown()
    {
        var (membership, auth, session) = Create();
        session.SignIn("alice");
        auth.ListSubjectGroupsAsync("alice", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(new LatticeAuthorizationDeniedException("not an administrator")));

        Assert.That(await membership.ReadAsync(), Is.SameAs(AppsCallerGroups.Unknown));
    }

    [Test]
    public async Task An_anonymous_caller_or_a_token_sign_in_is_unknown_without_a_read()
    {
        var (membership, auth, session) = Create();
        var anonymous = await membership.ReadAsync();
        session.SignIn("Alice Example", ExplorerAuthSchemes.Oidc);
        var token = await membership.ReadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(anonymous, Is.SameAs(AppsCallerGroups.Unknown));
            Assert.That(token, Is.SameAs(AppsCallerGroups.Unknown));
        });
        await auth.DidNotReceive().ListSubjectGroupsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_head_without_the_auth_facade_is_unknown()
    {
        var session = new FakeAuthSession();
        session.SignIn("explorer-admin");
        var services = new ServiceCollection().AddSingleton<IExplorerAuthSession>(session).BuildServiceProvider();

        Assert.That(await new AppsMembership(new AppsFacades(services)).ReadAsync(), Is.SameAs(AppsCallerGroups.Unknown));
    }

    [Test]
    public void A_cancelled_read_is_cancelled_rather_than_unknown()
    {
        var (membership, auth, session) = Create();
        session.SignIn("explorer-admin");
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        auth.ListSubjectGroupsAsync("explorer-admin", cancelled.Token).Returns(Task.FromCanceled<IReadOnlyList<string>>(cancelled.Token));

        Assert.That(async () => await membership.ReadAsync(cancelled.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [TestCase("")]
    [TestCase(" \t")]
    public async Task ReadAsync_blank_basic_subject_is_unknown_without_a_facade_read(string subject)
    {
        var (membership, auth, session) = Create();
        session.SignIn(subject);

        Assert.That(await membership.ReadAsync(), Is.SameAs(AppsCallerGroups.Unknown));
        await auth.DidNotReceive().ListSubjectGroupsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadAsync_null_group_response_is_known_empty_membership()
    {
        var (membership, auth, session) = Create();
        session.SignIn("alice");
        auth.ListSubjectGroupsAsync("alice", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(null!));

        var groups = await membership.ReadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(groups.IsKnown, Is.True);
            Assert.That(groups.SubjectId, Is.EqualTo("alice"));
            Assert.That(groups.Groups, Is.Empty);
        });
    }

    [Test]
    public async Task ReadAsync_membership_changes_are_read_again_and_tokens_are_forwarded()
    {
        var (membership, auth, session) = Create();
        session.SignIn("alice");
        using var cancellation = new CancellationTokenSource();
        var returned = new List<string> { "readers" };
        auth.ListSubjectGroupsAsync("alice", cancellation.Token)
            .Returns(_ => Task.FromResult<IReadOnlyList<string>>(returned));

        var before = await membership.ReadAsync(cancellation.Token);
        returned.Add("operators");
        var after = await membership.ReadAsync(cancellation.Token);

        Assert.Multiple(() =>
        {
            Assert.That(before.Groups, Is.EquivalentTo(new[] { "readers" }), "the earlier snapshot does not alias the facade's mutable list");
            Assert.That(after.Groups, Is.EquivalentTo(new[] { "readers", "operators" }));
        });
        await auth.Received(2).ListSubjectGroupsAsync("alice", cancellation.Token);
    }

    [Test]
    public async Task ReadAsync_backend_cancellation_without_caller_cancellation_is_unknown()
    {
        var (membership, auth, session) = Create();
        session.SignIn("alice");
        auth.ListSubjectGroupsAsync("alice", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(new OperationCanceledException()));

        Assert.That(await membership.ReadAsync(), Is.SameAs(AppsCallerGroups.Unknown));
    }

    [Test]
    public void ReadAsync_out_of_memory_is_not_disguised_as_unknown_membership()
    {
        var (membership, auth, session) = Create();
        session.SignIn("alice");
        var failure = new OutOfMemoryException("synthetic failure; no allocation pressure");
        auth.ListSubjectGroupsAsync("alice", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(failure));

        Assert.That(async () => await membership.ReadAsync(), Throws.Exception.SameAs(failure));
    }

    private static (AppsMembership Membership, ILatticeAuthAdmin Auth, FakeAuthSession Session) Create()
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        var session = new FakeAuthSession();
        var services = new ServiceCollection()
            .AddSingleton<IExplorerAuthSession>(session)
            .AddKeyedSingleton(ShellFacades.Key, auth)
            .BuildServiceProvider();
        return (new AppsMembership(new AppsFacades(services)), auth, session);
    }
}
