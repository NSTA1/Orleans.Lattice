using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The cluster group sources (#5370): the groups stored in Access, listed through
/// the auth facade's catalogue with an exact look-up past a full page, and the
/// directory's and stored groups in one list, each labelled, with every tenant
/// group id left out.
/// </summary>
[TestFixture]
public sealed class ClusterGroupSuggestionSourceTests
{
    [Test]
    public async Task Stored_groups_are_matched_as_typed_with_their_display_names_and_no_tenant_group()
    {
        var admin = new FakeAuthAdmin().WithGroup("data-stewards", "Data stewards").WithGroup("dev").WithGroup("t/acme/ops");

        var answer = await new StoredGroupSuggestionSource(new AccessCatalog(admin)).SuggestAsync(" d ", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "data-stewards", "dev" }));
            Assert.That(answer.Items[0].Detail, Is.EqualTo("Data stewards"));
            Assert.That(answer.Items[1].Detail, Is.Null);
            Assert.That(answer.Truncated, Is.False);
        });
    }

    [Test]
    public async Task A_full_first_page_looks_the_typed_id_up_exactly()
    {
        var admin = new FakeAuthAdmin();
        for (var i = 0; i < AuthPageRequest.MaxPageSize; i++)
        {
            admin.WithGroup($"a-{i:D4}");
        }

        admin.WithGroup("zz-late", "Late");
        var source = new StoredGroupSuggestionSource(new AccessCatalog(admin));

        var found = await source.SuggestAsync("zz-late", 5, CancellationToken.None);
        var missing = await source.SuggestAsync("zz-none", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new[] { ("zz-late", (string?)"Late") }));
            Assert.That(found.Truncated, Is.True);
            Assert.That(missing.Items, Is.Empty);
            Assert.That(missing.Truncated, Is.True);
            Assert.That(admin.Calls.Count(call => call == nameof(FakeAuthAdmin.GetGroupAsync)), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Without_a_catalogue_or_with_a_refused_listing_stored_groups_are_unavailable()
    {
        var refused = new FakeAuthAdmin();
        refused.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new LatticeAuthorizationDeniedException("no"));

        var none = await new StoredGroupSuggestionSource(null).SuggestAsync("x", 5, CancellationToken.None);
        var denied = await new StoredGroupSuggestionSource(new AccessCatalog(refused)).SuggestAsync("x", 5, CancellationToken.None);
        var nothing = await new StoredGroupSuggestionSource(null).SuggestAsync("x", 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(none.UnavailableReason, Is.EqualTo(StoredGroupSuggestionSource.UnavailableReason));
            Assert.That(denied.UnavailableReason, Is.EqualTo(StoredGroupSuggestionSource.UnavailableReason));
            Assert.That(nothing.IsAvailable, Is.True);
            Assert.That(nothing.Items, Is.Empty);
        });
    }

    [Test]
    public void A_cancelled_listing_propagates()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        var admin = new FakeAuthAdmin();
        admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new OperationCanceledException(cancelled.Token));

        Assert.That(
            async () => await new StoredGroupSuggestionSource(new AccessCatalog(admin)).SuggestAsync("x", 5, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Directory_and_stored_groups_are_listed_once_each_labelled_with_the_exact_id_first()
    {
        var directory = Source(LtSuggestionSet.Of([new("ops", "Operations"), new("eng", null), new("t/acme/x", null)]));
        var stored = Source(LtSuggestionSet.Of([new("ops", "Ops (stored)"), new("data-stewards", "Data stewards"), new("t/acme/y", null)]));

        var answer = await new ClusterGroupSuggestionSource(directory, stored).SuggestAsync("data-stewards", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[]
            {
                ("data-stewards", "Stored - Data stewards"),
                ("ops", "Directory and stored - Operations"),
                ("eng", "Directory"),
            }));
            Assert.That(answer.Truncated, Is.False);
        });
    }

    [Test]
    public async Task A_source_that_cannot_answer_is_left_out_and_two_that_cannot_give_the_directorys_reason()
    {
        var down = Source(LtSuggestionSet.Unavailable("Directory down."));
        var stored = Source(LtSuggestionSet.Of([new("data-stewards", null)]));
        var directory = Source(LtSuggestionSet.Of([new("ops", null)]));

        var storedOnly = await new ClusterGroupSuggestionSource(down, stored).SuggestAsync("a", 10, CancellationToken.None);
        var directoryOnly = await new ClusterGroupSuggestionSource(directory, Source(LtSuggestionSet.Unavailable("Store down."))).SuggestAsync("o", 10, CancellationToken.None);
        var neither = await new ClusterGroupSuggestionSource(down, Source(LtSuggestionSet.Unavailable("Store down."))).SuggestAsync("o", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(storedOnly.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[] { ("data-stewards", "Stored") }));
            Assert.That(directoryOnly.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[] { ("ops", "Directory") }));
            Assert.That(neither.UnavailableReason, Is.EqualTo("Directory down."));
        });
    }

    [Test]
    public async Task The_limit_bounds_the_merged_list_and_reports_truncation()
    {
        var directory = Source(LtSuggestionSet.Of([new("a", null), new("b", null)]));
        var stored = Source(LtSuggestionSet.Of([new("c", null)]));
        var source = new ClusterGroupSuggestionSource(directory, stored);

        var two = await source.SuggestAsync("", 2, CancellationToken.None);
        var none = await source.SuggestAsync("", 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(two.Items.Select(item => item.Value), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(two.Truncated, Is.True);
            Assert.That(none.Items, Is.Empty);
            Assert.That(ClusterGroupSuggestionSource.Labels, Is.EqualTo(new[] { "Directory and stored", "Directory", "Stored" }));
        });
    }

    [Test]
    public void Null_text_is_refused()
    {
        var source = new ClusterGroupSuggestionSource(Source(LtSuggestionSet.Empty), Source(LtSuggestionSet.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await source.SuggestAsync(null!, 5, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(async () => await new StoredGroupSuggestionSource(null).SuggestAsync(null!, 5, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_hub_lists_both_sources_through_the_circuits_catalogue()
    {
        var admin = new FakeAuthAdmin().WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group).WithGroup("data-stewards", "Data stewards");
        var directory = new Orleans.Lattice.Explorer.UI.Suggestions.DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group);

        var answer = await new ClusterGroupSuggestionSource(directory, new StoredGroupSuggestionSource(new AccessCatalog(admin))).SuggestAsync("", 10, CancellationToken.None);

        Assert.That(answer.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[]
        {
            ("ops", "Directory - Operations"),
            ("data-stewards", "Stored - Data stewards"),
        }));
    }

    private static ILtSuggestionSource Source(LtSuggestionSet answer) => new FixedSource(answer);

    private sealed class FixedSource(LtSuggestionSet answer) : ILtSuggestionSource
    {
        public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken) => ValueTask.FromResult(answer);
    }
}
