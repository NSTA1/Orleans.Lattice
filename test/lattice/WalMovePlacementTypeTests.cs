using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public WAL placement value types and exceptions exposed
/// through the <see cref="ILatticeAdmin"/> move surface: <see cref="WalMoveOptions"/>,
/// <see cref="WalMoveOutcome"/>, <see cref="LatticeWalProviderMissingException"/>,
/// and <see cref="LatticeWalQuiescingException"/>.
/// </summary>
[TestFixture]
public sealed class WalMovePlacementTypeTests
{
    [Test]
    public void WalMoveOptions_Default_uses_documented_defaults()
    {
        var defaults = WalMoveOptions.Default;

        Assert.That(defaults.QuiesceLease, Is.EqualTo(WalMoveOptions.DefaultQuiesceLease));
        Assert.That(defaults.CopyPageSize, Is.EqualTo(WalMoveOptions.DefaultCopyPageSize));
        Assert.That(defaults.VerifyAfterCopy, Is.True);
    }

    [Test]
    public void WalMoveOptions_effective_lease_substitutes_default_for_unset_value()
    {
        var unset = new WalMoveOptions();

        Assert.That(unset.EffectiveQuiesceLease, Is.EqualTo(WalMoveOptions.DefaultQuiesceLease));
        Assert.That(unset.EffectiveCopyPageSize, Is.EqualTo(WalMoveOptions.DefaultCopyPageSize));
    }

    [Test]
    public void WalMoveOptions_effective_values_honour_explicit_overrides()
    {
        var custom = new WalMoveOptions
        {
            QuiesceLease = TimeSpan.FromSeconds(5),
            CopyPageSize = 32,
        };

        Assert.That(custom.EffectiveQuiesceLease, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(custom.EffectiveCopyPageSize, Is.EqualTo(32));
    }

    [Test]
    public void WalMoveOutcome_has_stable_ordinal_wire_values()
    {
        Assert.That((int)WalMoveOutcome.Moved, Is.EqualTo(0));
        Assert.That((int)WalMoveOutcome.AlreadyAtTarget, Is.EqualTo(1));
        Assert.That((int)WalMoveOutcome.SourceReclaimed, Is.EqualTo(2));
        Assert.That((int)WalMoveOutcome.NoOp, Is.EqualTo(3));
    }

    [Test]
    public void LatticeWalProviderMissingException_carries_tree_partition_and_key()
    {
        var ex = new LatticeWalProviderMissingException("tree-x", 3, "acct-b");

        Assert.That(ex.TreeId, Is.EqualTo("tree-x"));
        Assert.That(ex.Partition, Is.EqualTo(3));
        Assert.That(ex.ProviderKey, Is.EqualTo("acct-b"));
        Assert.That(ex.Message, Does.Contain("acct-b"));
    }

    [Test]
    public void LatticeWalQuiescingException_message_round_trips()
    {
        var ex = new LatticeWalQuiescingException("quiescing for move");

        Assert.That(ex.Message, Is.EqualTo("quiescing for move"));
    }
}
