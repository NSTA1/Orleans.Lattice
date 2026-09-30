using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The defined groups a new group's id is checked against: read through the
/// catalogue's caller-keyed memo, looked up exactly when the listed page may not
/// hold every group, and unavailable - never a refusal - when they cannot be read.
/// </summary>
[TestFixture]
public sealed class AccessGroupNameSourceTests
{
    [Test]
    public async Task A_defined_group_is_found_and_the_catalogue_is_read_once()
    {
        var admin = new FakeAuthAdmin().WithGroup("ops", "Operations").WithGroup("sales");
        var source = new AccessGroupNameSource(new AccessCatalog(admin));

        var found = await source.SuggestAsync("ops", 8, CancellationToken.None);
        var missing = await source.SuggestAsync("auditors", 8, CancellationToken.None);
        var blank = await source.SuggestAsync("  ", 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found.Find("ops")?.Detail, Is.EqualTo("Operations"));
            Assert.That(missing.IsAvailable, Is.True);
            Assert.That(missing.Items, Is.Empty);
            Assert.That(blank.Items, Is.Empty);
            Assert.That(admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(1), "memoised in the catalogue");
            Assert.That(admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.GetGroupAsync)), "a short page is the whole catalogue");
        });
    }

    [Test]
    public async Task When_the_listed_page_is_full_the_id_is_looked_up_exactly()
    {
        var admin = new FakeAuthAdmin();
        for (var i = 0; i < AuthPageRequest.MaxPageSize; i++)
        {
            admin.WithGroup($"a{i:D4}");
        }

        admin.WithGroup("zz-late");
        var source = new AccessGroupNameSource(new AccessCatalog(admin));

        var found = await source.SuggestAsync("zz-late", 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found.Find("zz-late"), Is.Not.Null);
            Assert.That(admin.Calls, Does.Contain(nameof(FakeAuthAdmin.GetGroupAsync)));
        });
    }

    [Test]
    public async Task Groups_that_cannot_be_read_are_unavailable_not_a_refusal()
    {
        var admin = new FakeAuthAdmin();
        admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new InvalidOperationException("down"));

        var answer = await new AccessGroupNameSource(new AccessCatalog(admin)).SuggestAsync("ops", 8, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(AccessGroupNameSource.UnavailableReason));
    }

    [Test]
    public void A_cancelled_read_propagates()
    {
        var admin = new FakeAuthAdmin();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new OperationCanceledException(cancelled.Token));

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await new AccessGroupNameSource(new AccessCatalog(admin)).SuggestAsync("ops", 8, cancelled.Token));
    }
}
