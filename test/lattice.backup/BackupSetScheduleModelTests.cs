namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The pure parts of backup-set scheduling (issue #6067): request validation,
/// the next-run arithmetic, whole-set retention, all-trees candidate filtering,
/// and the storage-safe schedule key.
/// </summary>
[TestFixture]
public sealed class BackupSetScheduleModelTests
{
    private static readonly IReadOnlyList<BackupScopeSelector> TwoTrees =
        [BackupScopeSelector.WholeTree("a"), BackupScopeSelector.WholeTree("b")];

    [Test]
    public void ForTrees_builds_an_explicit_schedule()
    {
        var request = LatticeBackupSetScheduleRequest.ForTrees("nightly", TwoTrees, TimeSpan.FromHours(12), 3, null);

        Assert.Multiple(() =>
        {
            Assert.That(request.Membership, Is.EqualTo(BackupSetMembershipMode.Explicit));
            Assert.That(request.Scopes, Has.Count.EqualTo(2));
            Assert.That(request.CrossTreeConsistent, Is.True);
            Assert.That(request.IncludeNewTrees, Is.False);
            Assert.That(request.RetentionKeepLast, Is.EqualTo(3));
        });
    }

    [Test]
    public void ForAllTrees_builds_an_all_trees_schedule()
    {
        var request = LatticeBackupSetScheduleRequest.ForAllTrees(
            "app", TimeSpan.FromHours(6), includeNewTrees: true, retentionKeepLast: null,
            retentionMaxAge: TimeSpan.FromDays(14), excludedTreePrefixes: ["scratch-"]);

        Assert.Multiple(() =>
        {
            Assert.That(request.Membership, Is.EqualTo(BackupSetMembershipMode.AllTrees));
            Assert.That(request.Scopes, Is.Empty);
            Assert.That(request.IncludeNewTrees, Is.True);
            Assert.That(request.ExcludedTreePrefixes, Is.EqualTo(new[] { "scratch-" }));
            Assert.That(request.RetentionMaxAge, Is.EqualTo(TimeSpan.FromDays(14)));
        });
    }

    [Test]
    public void Constructor_requires_a_retention_bound()
    {
        Assert.That(
            () => LatticeBackupSetScheduleRequest.ForTrees("nightly", TwoTrees, TimeSpan.FromHours(1), null, null),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_rejects_two_scopes_for_one_tree_and_scopes_on_an_all_trees_schedule()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => LatticeBackupSetScheduleRequest.ForTrees(
                    "n", [BackupScopeSelector.WholeTree("a"), BackupScopeSelector.Prefix("a", "p")], TimeSpan.FromHours(1), 1, null),
                Throws.ArgumentException);
            Assert.That(
                () => new LatticeBackupSetScheduleRequest("n", BackupSetMembershipMode.AllTrees, TwoTrees, TimeSpan.FromHours(1), 1, null),
                Throws.ArgumentException);
            Assert.That(
                () => new LatticeBackupSetScheduleRequest(
                    "n", BackupSetMembershipMode.Explicit, TwoTrees, TimeSpan.FromHours(1), 1, null, excludedTreePrefixes: ["x"]),
                Throws.ArgumentException);
            Assert.That(
                () => LatticeBackupSetScheduleRequest.ForTrees("n", TwoTrees, TimeSpan.Zero, 1, null),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => LatticeBackupSetScheduleRequest.ForTrees("n", TwoTrees, TimeSpan.FromHours(1), 0, null),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCase("")]
    [TestCase("has space")]
    [TestCase("t/tenant/name")]
    public void ValidateName_rejects_an_invalid_name(string name)
    {
        Assert.That(() => LatticeBackupSetScheduleRequest.ValidateName(name), Throws.ArgumentException);
    }

    [Test]
    public void ValidateName_rejects_an_over_long_name_and_accepts_a_valid_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => LatticeBackupSetScheduleRequest.ValidateName(new string('a', LatticeBackupSetScheduleRequest.MaxNameLength + 1)),
                Throws.ArgumentException);
            Assert.That(() => LatticeBackupSetScheduleRequest.ValidateName("app-1_nightly.v2"), Throws.Nothing);
        });
    }

    [Test]
    public void NextRun_is_one_period_after_the_anchor_then_on_each_period_boundary()
    {
        var anchor = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var period = TimeSpan.FromHours(6);

        Assert.Multiple(() =>
        {
            Assert.That(BackupSetSchedulerGrain.NextRun(anchor, period, anchor.AddHours(1)), Is.EqualTo(anchor.AddHours(6)));
            Assert.That(BackupSetSchedulerGrain.NextRun(anchor, period, anchor.AddHours(6)), Is.EqualTo(anchor.AddHours(12)));
            Assert.That(BackupSetSchedulerGrain.NextRun(anchor, period, anchor.AddHours(13)), Is.EqualTo(anchor.AddHours(18)));
        });
    }

    [Test]
    public void Retained_always_keeps_the_newest_and_honours_keep_last_and_max_age()
    {
        var now = DateTimeOffset.UtcNow;
        var sets = new[] { Set("s0", now.AddDays(-20)), Set("s1", now.AddDays(-30)), Set("s2", now.AddDays(-40)) }
            .OrderByDescending(s => s.LastCapturedAtUtc).ToList();

        var byAge = BackupSetSchedulerGrain.Retained(sets, null, TimeSpan.FromDays(14), now);
        var byCount = BackupSetSchedulerGrain.Retained(sets, 2, null, now);
        var union = BackupSetSchedulerGrain.Retained(sets, 1, TimeSpan.FromDays(35), now);

        Assert.Multiple(() =>
        {
            Assert.That(byAge.Select(s => s.SetId), Is.EqualTo(new[] { "s0" }), "the newest is kept even past the age bound");
            Assert.That(byCount.Select(s => s.SetId), Is.EqualTo(new[] { "s0", "s1" }));
            Assert.That(union.Select(s => s.SetId), Is.EqualTo(new[] { "s0", "s1" }));
        });
    }

    [Test]
    public void IsCandidate_keeps_user_trees_in_the_tenant_and_drops_reserved_and_excluded_ones()
    {
        var none = Array.Empty<string>();
        Assert.Multiple(() =>
        {
            Assert.That(BackupSetMembership.IsCandidate("orders", null, none), Is.True);
            Assert.That(BackupSetMembership.IsCandidate("t/acme/orders", null, none), Is.False, "another tenant's tree");
            Assert.That(BackupSetMembership.IsCandidate("t/acme/orders", "t/acme/", none), Is.True);
            Assert.That(BackupSetMembership.IsCandidate("orders", "t/acme/", none), Is.False);
            Assert.That(BackupSetMembership.IsCandidate("sys-backup-catalog", null, none), Is.False);
            Assert.That(BackupSetMembership.IsCandidate("_lattice_trees", null, none), Is.False);
            Assert.That(BackupSetMembership.IsCandidate("tag-color", null, none), Is.False);
            Assert.That(BackupSetMembership.IsCandidate("scratch-1", null, ["scratch-"]), Is.False);
        });
    }

    [Test]
    public void BackupSetScheduleKey_is_storage_safe_and_distinguishes_tenants()
    {
        var bare = BackupSetScheduleKey.For("nightly");
        var tenant = BackupSetScheduleKey.For("t/acme/nightly");

        Assert.Multiple(() =>
        {
            Assert.That(tenant, Does.Not.Contain("/"));
            Assert.That(bare, Is.Not.EqualTo(tenant));
            Assert.That(bare, Is.Not.EqualTo(BackupScopeKey.For(BackupScopeSelector.WholeTree("nightly"))));
        });
    }

    private static ScheduledBackupSet Set(string id, DateTimeOffset at) =>
        new(id, id, at, at, crossTreeConsistent: true, [new ScheduledBackupSetMember(BackupScopeSelector.WholeTree("a"), "b-" + id)]);
}
