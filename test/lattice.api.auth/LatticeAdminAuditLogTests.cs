using NSubstitute;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// The operator admin audit reader (#3638): admin gated, every scope, filtered, paged
/// newest first, and the only reader that shows in-flight intents as pending.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeAdminAuditLogTests
{
    private const string Acme = "t/acme";

    private static LatticeAdminAuditLog Create(AdminAuditTreeFake tree, bool allow = true, ILatticeAdminAuditTrail? trail = null) =>
        new(new Gate(allow), new Operator(), tree.GrainFactory, trail);

    private static AdminAuditTreeFake Seeded()
    {
        var tree = new AdminAuditTreeFake();
        tree.Final(AdminAuditTreeFake.Record("a", 1));
        tree.Final(AdminAuditTreeFake.Record("b", 2, scope: Acme, action: LatticeAdminAuditAction.TenantAdministratorAdd));
        tree.Final(AdminAuditTreeFake.Record("c", 3, outcome: LatticeAdminAuditOutcome.Denied, actor: "mallory"));
        tree.Final(AdminAuditTreeFake.Record("d", 4, scope: Acme));
        tree.Final(AdminAuditTreeFake.Record("e", 5));
        return tree;
    }

    private static string[] Ids(AdminAuditPage page) => page.Records.Select(r => r.OperationId).ToArray();

    [Test]
    public async Task ListAuditAsync_returns_every_scope_newest_first()
    {
        var page = await Create(Seeded()).ListAuditAsync(new AdminAuditQuery());

        Assert.Multiple(() =>
        {
            Assert.That(Ids(page), Is.EqualTo(new[] { "e", "d", "c", "b", "a" }));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListAuditAsync_applies_the_scope_time_actor_action_and_outcome_filters()
    {
        var log = Create(Seeded());
        var minute = (int m) => new DateTimeOffset(2026, 11, 27, 12, m, 0, TimeSpan.Zero);

        var scoped = await log.ListAuditAsync(new AdminAuditQuery { Scope = Acme });
        var window = await log.ListAuditAsync(new AdminAuditQuery { From = minute(2), To = minute(4) });
        var actor = await log.ListAuditAsync(new AdminAuditQuery { Actor = "mallory" });
        var action = await log.ListAuditAsync(new AdminAuditQuery { Action = LatticeAdminAuditAction.TenantAdministratorAdd });
        var outcome = await log.ListAuditAsync(new AdminAuditQuery { Outcome = LatticeAdminAuditOutcome.Denied });
        var scopedWindow = await log.ListAuditAsync(new AdminAuditQuery { Scope = Acme, From = minute(3) });

        Assert.Multiple(() =>
        {
            Assert.That(Ids(scoped), Is.EqualTo(new[] { "d", "b" }));
            Assert.That(Ids(window), Is.EqualTo(new[] { "c", "b" }));
            Assert.That(Ids(actor), Is.EqualTo(new[] { "c" }));
            Assert.That(Ids(action), Is.EqualTo(new[] { "b" }));
            Assert.That(Ids(outcome), Is.EqualTo(new[] { "c" }));
            Assert.That(Ids(scopedWindow), Is.EqualTo(new[] { "d" }));
        });
    }

    [TestCase(null)]
    [TestCase(Acme)]
    public async Task ListAuditAsync_pages_with_the_token_until_exhausted(string? scope)
    {
        var log = Create(Seeded());
        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await log.ListAuditAsync(new AdminAuditQuery { Scope = scope, PageSize = 2, PageToken = token });
            seen.AddRange(Ids(page));
            token = page.NextPageToken;
            pages++;
        }
        while (token is not null && pages < 10);

        Assert.That(seen, Is.EqualTo(scope is null ? new[] { "e", "d", "c", "b", "a" } : new[] { "d", "b" }));
    }

    [Test]
    public async Task ListAuditAsync_shows_in_flight_intents_as_pending_once()
    {
        var tree = Seeded();
        tree.Pending(AdminAuditTreeFake.Record("p", 6, outcome: LatticeAdminAuditOutcome.Committed));
        tree.Pending(AdminAuditTreeFake.Record("e", 5));

        var page = await Create(tree).ListAuditAsync(new AdminAuditQuery());

        Assert.Multiple(() =>
        {
            Assert.That(Ids(page), Is.EqualTo(new[] { "p", "e", "d", "c", "b", "a" }));
            Assert.That(page.Records[0].Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Pending));
            Assert.That(page.Records[1].Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Committed));
        });
    }

    [Test]
    public async Task ListAuditAsync_does_not_show_a_finished_call_as_pending_when_its_final_record_is_after_To()
    {
        var tree = Seeded();
        tree.Pending(AdminAuditTreeFake.Record("late", 6, scope: Acme));
        tree.Final(AdminAuditTreeFake.Record("late", 8, scope: Acme));
        var to = new DateTimeOffset(2026, 11, 27, 12, 7, 0, TimeSpan.Zero);

        var page = await Create(tree).ListAuditAsync(new AdminAuditQuery { Scope = Acme, To = to });

        Assert.That(Ids(page), Is.EqualTo(new[] { "d", "b" }));
    }

    [Test]
    public async Task ListAuditAsync_does_not_show_a_finished_call_as_pending_when_its_final_record_is_past_the_cursor()
    {
        var tree = Seeded();
        tree.Pending(AdminAuditTreeFake.Record("late", 3, scope: Acme));
        tree.Final(AdminAuditTreeFake.Record("late", 6, scope: Acme));
        var log = Create(tree);

        var first = await log.ListAuditAsync(new AdminAuditQuery { Scope = Acme, PageSize = 1 });
        var second = await log.ListAuditAsync(new AdminAuditQuery { Scope = Acme, PageSize = 1, PageToken = first.NextPageToken });
        var third = await log.ListAuditAsync(new AdminAuditQuery { Scope = Acme, PageSize = 1, PageToken = second.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(Ids(first), Is.EqualTo(new[] { "late" }));
            Assert.That(Ids(second), Is.EqualTo(new[] { "d" }));
            Assert.That(Ids(third), Is.EqualTo(new[] { "b" }));
            Assert.That(third.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListAuditAsync_still_shows_a_scoped_in_flight_intent_as_pending()
    {
        var tree = Seeded();
        tree.Pending(AdminAuditTreeFake.Record("live", 3, scope: Acme));
        var to = new DateTimeOffset(2026, 11, 27, 12, 7, 0, TimeSpan.Zero);

        var page = await Create(tree).ListAuditAsync(new AdminAuditQuery { Scope = Acme, To = to });

        Assert.Multiple(() =>
        {
            Assert.That(Ids(page), Is.EqualTo(new[] { "d", "live", "b" }));
            Assert.That(page.Records[1].Outcome, Is.EqualTo(LatticeAdminAuditOutcome.Pending));
        });
    }

    [Test]
    public async Task ListAuditAsync_on_a_cluster_without_a_trail_returns_an_empty_page()
    {
        var page = await Create(new AdminAuditTreeFake(treeExists: false)).ListAuditAsync(new AdminAuditQuery());

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void ListAuditAsync_refuses_a_non_admin_and_records_the_denial()
    {
        var trail = Substitute.For<ILatticeAdminAuditTrail>();
        trail.IsEnabled.Returns(true);
        var log = Create(Seeded(), allow: false, trail: trail);

        Assert.That(async () => await log.ListAuditAsync(new AdminAuditQuery()), Throws.InstanceOf<UnauthorizedAccessException>());
        _ = trail.Received(1).RecordDeniedAsync(
            Arg.Is<LatticeAdminAuditRequest>(r => r.Action == LatticeAdminAuditAction.Read),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void ListAuditAsync_rejects_a_null_query_and_a_malformed_token()
    {
        var log = Create(Seeded());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await log.ListAuditAsync(null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await log.ListAuditAsync(new AdminAuditQuery { PageToken = "garbage" }),
                Throws.ArgumentException);
        });
    }

    private sealed class Gate(bool allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied"));
    }

    private sealed class Operator : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(new LatticeSubject("op"));
    }
}
