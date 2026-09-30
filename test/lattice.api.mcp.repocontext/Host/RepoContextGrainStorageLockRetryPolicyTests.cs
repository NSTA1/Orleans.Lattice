using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextGrainStorageLockRetryPolicy"/>, which decides which
/// grain-storage lock failures the host re-issues (issues #3761 item 6, #2419).
/// </summary>
/// <remarks>
/// The prefixes it admits are pinned against the core's own declared state names by
/// <see cref="RepoContextGrainStorageLockRetryPolicyStateNameTests"/>; what is covered
/// here is the matching, the bound, and the backoff.
/// </remarks>
[TestFixture]
public sealed class RepoContextGrainStorageLockRetryPolicyTests
{
    [Test]
    public void The_narrow_pin_state_policy_re_issues_pin_state_writes_and_clears_only()
    {
        var policy = RepoContextGrainStorageLockRetryPolicy.PinStateWrites;

        Assert.Multiple(() =>
        {
            Assert.That(policy.MaxRetries, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultMaxRetries));
            Assert.That(policy.BaseDelay, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultBaseDelay));
            Assert.That(policy.StateNamePrefixes,
                Is.EqualTo(new[] { RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix }));
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins~b2"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Clear, "wal-materialiser-pins~b15"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Read, "wal-materialiser-pins~b15"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf"), Is.False,
                "This is the state of the world issue #2419 was filed against: the leaf's checkpoint "
                + "advance was not re-issued. It is kept as a fixture so the widening below is a "
                + "measured difference rather than an assertion about itself.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "WAL-MATERIALISER-PINS"), Is.False,
                "The match is ordinal, like the state names themselves.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, null!), Is.False);
        });
    }

    [Test]
    public void The_host_default_re_issues_every_write_whose_loss_generates_more_writes()
    {
        var policy = RepoContextGrainStorageLockRetryPolicy.SelfAmplifyingWrites;

        Assert.Multiple(() =>
        {
            Assert.That(policy.MaxRetries, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultMaxRetries));
            Assert.That(policy.BaseDelay, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultBaseDelay));

            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf"), Is.True,
                "The leaf's write is the durable checkpoint advance. Dropping it unretried is what "
                + "makes the leaf re-enter replay over the same partition gap and issue the next "
                + "burst of writes - the self-sustaining half of issue #2419.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "shardroot"), Is.True,
                "The state the attribution line names, and the convoy's largest victim by volume.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins~b3"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf-snapshot"), Is.True,
                "Covered deliberately: a leaf that loses its snapshot activates cold and replays its "
                + "whole readable WAL window, which is the same amplification by another route.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Clear, "shardroot"), Is.True);

            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Read, "leaf"), Is.False,
                "A WAL-mode reader does not queue for the writer lock, so a failed read is the "
                + "caller's to retry at no risk to stored state.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "internal"), Is.False,
                "The set is an allow-list of self-amplifying writes, not everything. A state whose "
                + "loss does not generate more writes stays outside it.");
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "tree-snapshot"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, null!), Is.False);
        });
    }

    [Test]
    public void The_none_policy_re_issues_nothing()
    {
        var policy = RepoContextGrainStorageLockRetryPolicy.None;

        Assert.Multiple(() =>
        {
            Assert.That(policy.MaxRetries, Is.Zero);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins~b2"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf"), Is.False);
        });
    }

    [Test]
    public void A_policy_with_no_prefixes_re_issues_nothing()
    {
        var policy = new RepoContextGrainStorageLockRetryPolicy(3, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(policy.StateNamePrefixes, Is.Empty);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf"), Is.False,
                "An empty allow-list admits nothing, rather than degenerating into admitting all.");
        });
    }

    [Test]
    public void A_policy_matches_any_of_its_prefixes_and_copies_them_defensively()
    {
        var prefixes = new[] { "alpha", "beta" };
        var policy = new RepoContextGrainStorageLockRetryPolicy(1, TimeSpan.Zero, prefixes);
        prefixes[0] = "mutated";

        Assert.Multiple(() =>
        {
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "alpha-1"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "beta"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "gamma"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "mutated"), Is.False,
                "The policy copies the array, so a caller cannot widen an in-use policy afterwards.");
        });
    }

    [Test]
    public void The_delay_doubles_per_re_issue_and_is_jittered_between_half_and_all_of_it()
    {
        var policy = new RepoContextGrainStorageLockRetryPolicy(3, TimeSpan.FromMilliseconds(200), "p");

        Assert.Multiple(() =>
        {
            Assert.That(policy.DelayFor(1, 0d), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(policy.DelayFor(1, 1d), Is.EqualTo(TimeSpan.FromMilliseconds(200)));
            Assert.That(policy.DelayFor(2, 0d), Is.EqualTo(TimeSpan.FromMilliseconds(200)));
            Assert.That(policy.DelayFor(2, 1d), Is.EqualTo(TimeSpan.FromMilliseconds(400)));
            Assert.That(policy.DelayFor(3, 0.5d), Is.EqualTo(TimeSpan.FromMilliseconds(600)));
            Assert.That(policy.DelayFor(1, 7d), Is.EqualTo(TimeSpan.FromMilliseconds(200)), "Jitter is clamped to [0, 1].");
            Assert.That(policy.DelayFor(1, -7d), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(policy.DelayFor(64, 1d), Is.EqualTo(TimeSpan.FromMilliseconds(200) * 65_536),
                "The doubling is capped so a large retry number cannot overflow.");
        });
    }

    [Test]
    public void The_constructor_and_delay_reject_invalid_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new RepoContextGrainStorageLockRetryPolicy(-1, TimeSpan.Zero, "p"));
            Assert.Throws<ArgumentOutOfRangeException>(() => new RepoContextGrainStorageLockRetryPolicy(1, TimeSpan.FromTicks(-1), "p"));
            Assert.Throws<ArgumentNullException>(() => new RepoContextGrainStorageLockRetryPolicy(1, TimeSpan.Zero, (string[])null!));
            Assert.Throws<ArgumentNullException>(() => new RepoContextGrainStorageLockRetryPolicy(1, TimeSpan.Zero, "ok", null!));
            Assert.Throws<ArgumentOutOfRangeException>(() => RepoContextGrainStorageLockRetryPolicy.PinStateWrites.DelayFor(0, 0d));
        });
    }
}
