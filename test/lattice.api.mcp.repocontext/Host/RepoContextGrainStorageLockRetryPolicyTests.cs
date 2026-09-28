using System.Reflection;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextGrainStorageLockRetryPolicy"/>, which decides which
/// grain-storage lock failures the host re-issues (issue #3761 item 6).
/// </summary>
[TestFixture]
public sealed class RepoContextGrainStorageLockRetryPolicyTests
{
    [Test]
    public void The_pin_state_prefix_is_the_core_materialiser_pin_state_name()
    {
        var pinState = typeof(LatticeOptions).Assembly.GetType("Orleans.Lattice.BPlusTree.Grains.WalMaterialiserPinState", throwOnError: true)!;
        var stateName = pinState.GetField("StateName", BindingFlags.Public | BindingFlags.Static)!.GetRawConstantValue();

        Assert.That(RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix, Is.EqualTo(stateName),
            "The host names the pin store by prefix because the core type is internal. If the core "
            + "renames its state, pin writes would silently stop being re-issued.");
    }

    [Test]
    public void The_host_default_re_issues_pin_state_writes_and_clears_only()
    {
        var policy = RepoContextGrainStorageLockRetryPolicy.PinStateWrites;

        Assert.Multiple(() =>
        {
            Assert.That(policy.MaxRetries, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultPinStateMaxRetries));
            Assert.That(policy.BaseDelay, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.DefaultBaseDelay));
            Assert.That(policy.StateNamePrefix, Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix));
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "wal-materialiser-pins~b2"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Clear, "wal-materialiser-pins~b15"), Is.True);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Read, "wal-materialiser-pins~b15"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "leaf"), Is.False);
            Assert.That(policy.Applies(RepoContextGrainStorageOperation.Write, "WAL-MATERIALISER-PINS"), Is.False,
                "The match is ordinal, like the state names themselves.");
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
            Assert.Throws<ArgumentNullException>(() => new RepoContextGrainStorageLockRetryPolicy(1, TimeSpan.Zero, null!));
            Assert.Throws<ArgumentOutOfRangeException>(() => RepoContextGrainStorageLockRetryPolicy.PinStateWrites.DelayFor(0, 0d));
        });
    }
}
