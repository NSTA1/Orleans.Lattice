namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupRetentionPlanner"/>, the pure core of backup
/// retention pruning: the keep set (keep-last, max-age, the unbounded no-op), the
/// base closure, the retained artifacts, the dependent scan and the publish check
/// an incremental capture makes (issue #5357).
/// </summary>
[TestFixture]
public sealed class BackupRetentionPlannerTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 31, 12, 0, 0, TimeSpan.Zero);

    private static BackupManifest Full(string id, DateTimeOffset createdAt, string? artifact = null) =>
        BackupManifestModelTests.Sample(id) with
        {
            CreatedAtUtc = createdAt,
            ContentDescriptors = [Descriptor(artifact ?? "art-" + id)],
        };

    private static BackupManifest Incr(string id, string baseId, DateTimeOffset createdAt, string? artifact = null) =>
        BackupManifestModelTests.Sample(id, BackupKind.Incremental, baseId) with
        {
            CreatedAtUtc = createdAt,
            ContentDescriptors = [Descriptor(artifact ?? "art-" + id)],
        };

    private static BackupContentDescriptor Descriptor(string artifactId) =>
        new(artifactId, "hash", 10, 1, BackupScopeSelector.WholeTree("orders"));

    [Test]
    public void Neither_knob_set_retains_everything()
    {
        var listing = new[] { Full("a", Now.AddDays(-30)), Full("b", Now.AddDays(-1)) };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: null, maxAge: null, Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "a", "b" }));
        Assert.That(BackupRetentionPlanner.Doomed(listing, retained), Is.Empty);
    }

    [Test]
    public void A_republished_backup_ranks_by_its_latest_capture()
    {
        // Republished one day ago, first captured thirty days ago: keep-last-one
        // keeps it over a backup created two days ago, and a three-day max-age
        // keeps it too.
        var republished = Full("republished", Now.AddDays(-30)) with { LastCapturedAtUtc = Now.AddDays(-1) };
        var listing = new[] { republished, Full("newer-created", Now.AddDays(-2)) };

        Assert.Multiple(() =>
        {
            Assert.That(
                BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, Now),
                Is.EquivalentTo(new[] { "republished" }));
            Assert.That(
                BackupRetentionPlanner.ComputeRetained(listing, keepLast: null, maxAge: TimeSpan.FromDays(3), Now),
                Is.EquivalentTo(new[] { "republished", "newer-created" }));
            Assert.That(BackupRetentionPlanner.RankTime(republished), Is.EqualTo(Now.AddDays(-1)));
            Assert.That(BackupRetentionPlanner.RankTime(Full("legacy", Now.AddDays(-4))), Is.EqualTo(Now.AddDays(-4)), "a manifest without the field ranks by its creation time");
        });
    }

    [Test]
    public void Keep_last_keeps_the_newest_by_creation_time_then_ordinal_id()
    {
        var listing = new[]
        {
            Full("old", Now.AddDays(-3)),
            Full("tie-a", Now.AddDays(-1)),
            Full("tie-b", Now.AddDays(-1)),
            Full("mid", Now.AddDays(-2)),
        };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "tie-b" }));
        Assert.That(
            BackupRetentionPlanner.Doomed(listing, retained).Select(m => m.Id),
            Is.EqualTo(new[] { "old", "tie-a", "mid" }));
    }

    [Test]
    public void Max_age_keeps_a_backup_created_exactly_at_the_cutoff()
    {
        var listing = new[]
        {
            Full("at-cutoff", Now - TimeSpan.FromHours(1)),
            Full("past-cutoff", Now - TimeSpan.FromHours(1) - TimeSpan.FromTicks(1)),
        };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: null, maxAge: TimeSpan.FromHours(1), Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "at-cutoff" }));
    }

    [Test]
    public void Max_age_at_the_extreme_window_keeps_everything_without_faulting()
    {
        var listing = new[] { Full("a", DateTimeOffset.UnixEpoch) };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: null, maxAge: TimeSpan.MaxValue, Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "a" }));
    }

    [Test]
    public void A_backup_is_retained_when_either_knob_keeps_it()
    {
        var listing = new[]
        {
            Full("old", Now.AddDays(-10)),
            Full("young", Now.AddMinutes(-5)),
            Full("newest-but-old", Now.AddDays(-5)),
        };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: TimeSpan.FromHours(1), Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "young" }));

        listing = [Full("old", Now.AddDays(-10)), Full("newest-but-old", Now.AddDays(-5))];
        retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: TimeSpan.FromHours(1), Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "newest-but-old" }));
    }

    [Test]
    public void Max_age_keeps_a_backup_keep_last_does_not_when_both_knobs_are_set()
    {
        var listing = new[]
        {
            Full("old", Now.AddDays(-10)),
            Full("young", Now.AddMinutes(-30)),
            Full("newest", Now.AddMinutes(-5)),
        };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: TimeSpan.FromHours(1), Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "young", "newest" }));
    }

    [Test]
    public void The_base_chain_of_a_retained_increment_is_retained_transitively()
    {
        var listing = new[]
        {
            Full("b0", Now.AddDays(-3)),
            Incr("i1", "b0", Now.AddDays(-2)),
            Incr("i2", "i1", Now.AddDays(-1)),
            Full("other", Now.AddDays(-4)),
        };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "i2", "i1", "b0" }));
    }

    [Test]
    public void The_closure_follows_a_base_only_when_it_is_in_the_listing()
    {
        var listing = new[] { Incr("i1", "unlisted", Now) };

        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, Now);

        Assert.That(retained, Is.EquivalentTo(new[] { "i1" }));
    }

    [Test]
    public void An_artifact_a_retained_manifest_references_is_retained()
    {
        var listing = new[]
        {
            Full("pruned", Now.AddDays(-2), artifact: "shared"),
            Full("kept", Now, artifact: "shared"),
        };
        var retained = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, Now);

        var artifacts = BackupRetentionPlanner.RetainedArtifacts(listing, retained);

        Assert.That(artifacts, Is.EquivalentTo(new[] { "shared" }));
    }

    [Test]
    public void A_doomed_manifest_an_increment_outside_the_pass_builds_on_is_depended_on()
    {
        var b0 = Full("b0", Now.AddDays(-3));
        var b1 = Full("b1", Now.AddDays(-2));
        var sink = new[] { b0, Incr("in-flight", "b0", Now), b1 };

        var blocked = BackupRetentionPlanner.DependedOn(new[] { b0, b1 }, sink);

        Assert.That(blocked, Is.EquivalentTo(new[] { "b0" }));
    }

    [Test]
    public void The_base_chain_of_a_kept_manifest_is_kept_transitively()
    {
        var b0 = Full("b0", Now.AddDays(-3));
        var i1 = Incr("i1", "b0", Now.AddDays(-2));
        var sink = new[] { b0, i1, Incr("in-flight", "i1", Now) };

        var blocked = BackupRetentionPlanner.DependedOn(new[] { b0, i1 }, sink);

        Assert.That(blocked, Is.EquivalentTo(new[] { "b0", "i1" }));
    }

    [Test]
    public void An_increment_the_pass_is_itself_pruning_does_not_keep_its_base()
    {
        var b0 = Full("b0", Now.AddDays(-3));
        var i1 = Incr("i1", "b0", Now.AddDays(-2));

        var blocked = BackupRetentionPlanner.DependedOn(new[] { b0, i1 }, new[] { b0, i1 });

        Assert.That(blocked, Is.Empty);
    }

    [Test]
    public void A_manifest_nothing_builds_on_is_not_depended_on()
    {
        var b0 = Full("b0", Now);

        var blocked = BackupRetentionPlanner.DependedOn(new[] { b0 }, new[] { b0 });

        Assert.That(blocked, Is.Empty);
    }

    [Test]
    public void Deletion_runs_tip_first()
    {
        var b0 = Full("b0", Now.AddDays(-3));
        var i1 = Incr("i1", "b0", Now.AddDays(-2));
        var i2 = Incr("i2", "i1", Now.AddDays(-1));
        var other = Full("other", Now.AddDays(-4));

        var ordered = BackupRetentionPlanner.TipFirst(new[] { b0, i1, other, i2 }).Select(m => m.Id).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(ordered, Has.Count.EqualTo(4));
            Assert.That(ordered.IndexOf("i2"), Is.LessThan(ordered.IndexOf("i1")));
            Assert.That(ordered.IndexOf("i1"), Is.LessThan(ordered.IndexOf("b0")));
        });
    }

    [Test]
    public void An_increment_publishes_only_while_its_base_row_is_present()
    {
        var increment = Incr("i1", "b0", Now);

        Assert.Multiple(() =>
        {
            Assert.That(BackupRetentionPlanner.MayPublishIncrement(increment, Full("b0", Now.AddDays(-1))), Is.True);
            Assert.That(BackupRetentionPlanner.MayPublishIncrement(increment, null), Is.False);
            Assert.That(BackupRetentionPlanner.MayPublishIncrement(increment, Full("other", Now)), Is.False);
            Assert.That(BackupRetentionPlanner.MayPublishIncrement(Full("f", Now), null), Is.True);
        });
    }
}
