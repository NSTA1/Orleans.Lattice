using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Suggestions;

/// <summary>Directory search and exact-id resolution failure boundaries.</summary>
[TestFixture]
public sealed class DirectorySuggestionSourceTests
{
    [TestCase("search")]
    [TestCase("resolve")]
    public void SuggestAsync_caller_cancellation_propagates_from_each_read(string stage)
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        var fault = new OperationCanceledException(cancelled.Token);
        var admin = FailingAt(stage, fault);
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group);

        Assert.That(async () => await source.SuggestAsync("ops", 2, cancelled.Token), Throws.Exception.SameAs(fault));
    }

    [TestCase("search")]
    public async Task SuggestAsync_unrequested_cancellation_answers_unavailable(string stage)
    {
        var admin = FailingAt(stage, new OperationCanceledException("server stopped"));
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group);

        var answer = await source.SuggestAsync("ops", 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.False);
            Assert.That(answer.Items, Is.Empty);
            Assert.That(answer.UnavailableReason, Is.EqualTo(DirectorySuggestionSource.UnavailableReason));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SuggestAsync_exact_resolution_fault_keeps_the_search_page(bool cancellation)
    {
        Exception fault = cancellation ? new OperationCanceledException("server stopped") : new InvalidOperationException("lookup failed");
        var admin = FailingAt("resolve", fault);
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group);

        var answer = await source.SuggestAsync("ops", 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.True);
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "ops-east", "ops-west" }));
            Assert.That(answer.Truncated, Is.True);
        });
        await admin.Received(1).ResolveDirectoryPrincipalAsync("ops", CancellationToken.None);
    }

    [Test]
    public async Task SuggestAsync_truncated_page_containing_exact_id_does_not_resolve_again()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(Page("ops-east", "ops"));
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group);

        var answer = await source.SuggestAsync(" ops ", 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "ops", "ops-east" }));
            Assert.That(answer.Items[0].Detail, Is.Null, "the id is not repeated as its display name");
            Assert.That(answer.Truncated, Is.True);
        });
        await admin.DidNotReceiveWithAnyArgs().ResolveDirectoryPrincipalAsync(default!, default);
    }

    [TestCase("other", DirectoryPrincipalKind.Group)]
    [TestCase("ops", DirectoryPrincipalKind.User)]
    public async Task SuggestAsync_resolved_principal_must_match_both_id_and_kind(string id, DirectoryPrincipalKind kind)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(Page("ops-east", "ops-west"));
        admin.ResolveDirectoryPrincipalAsync("ops", Arg.Any<CancellationToken>())
            .Returns(new DirectoryPrincipalDescriptor { Id = id, DisplayName = id, Kind = kind });

        var answer = await new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group)
            .SuggestAsync("ops", 2, CancellationToken.None);

        Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "ops-east", "ops-west" }));
    }

    [Test]
    public async Task SuggestAsync_search_trims_text_caps_page_size_and_passes_the_token()
    {
        using var lifetime = new CancellationTokenSource();
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(new DirectorySearchResult { Available = true, Principals = [] });

        var answer = await new DirectorySuggestionSource(admin, kind: null).SuggestAsync(" ops ", 200, lifetime.Token);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.True);
            Assert.That(answer.Items, Is.Empty);
            Assert.That(answer.Truncated, Is.False);
        });
        await admin.Received(1).SearchDirectoryAsync(
            Arg.Is<DirectorySearchRequest>(request => request.Term == "ops" && request.PageSize == 100 && request.Kind == null),
            lifetime.Token);
    }

    [Test]
    public void SuggestAsync_null_text_is_rejected_before_any_read()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();

        Assert.That(async () => await new DirectorySuggestionSource(admin, null).SuggestAsync(null!, 2, CancellationToken.None),
            Throws.ArgumentNullException);
        Assert.That(admin.ReceivedCalls(), Is.Empty);
    }

    private static DirectorySearchResult Page(params string[] ids) => new()
    {
        Available = true,
        Principals = [.. ids.Select(id => new DirectoryPrincipalDescriptor { Id = id, DisplayName = id, Kind = DirectoryPrincipalKind.Group })],
        ContinuationToken = "next",
    };

    private static ILatticeAuthAdmin FailingAt(string stage, Exception fault)
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(stage switch
            {
                "search" => Task.FromException<DirectorySearchResult>(fault),
                "resolve" => Task.FromResult(Page("ops-east", "ops-west")),
                _ => throw new ArgumentOutOfRangeException(nameof(stage)),
            });
        admin.ResolveDirectoryPrincipalAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<DirectoryPrincipalDescriptor?>(fault));
        return admin;
    }
}
