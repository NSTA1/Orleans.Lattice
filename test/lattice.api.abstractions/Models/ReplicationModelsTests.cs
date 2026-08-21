using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the hand-written constructors, <see cref="ArgumentNullException"/>
/// guards, and static well-known instances of the replication control result
/// records. The serialization fixture only round-trips uninitialised instances,
/// so this construction logic is otherwise uncovered.
/// </summary>
[TestFixture]
public class ReplicationModelsTests
{
    [Test]
    public void ReplicationConfigReport_ctor_captures_trees()
    {
        var trees = new[] { new ReplicationTreeConfigEntry("t", true, LatticeMergeMode.LwwRegister, false) };

        var report = new ReplicationConfigReport(trees);

        Assert.That(report.Trees, Is.SameAs(trees));
    }

    [Test]
    public void ReplicationConfigReport_ctor_throws_for_null_trees()
        => Assert.That(() => new ReplicationConfigReport(null!), Throws.ArgumentNullException);

    [Test]
    public void ReplicationConfigReport_Empty_is_an_empty_report()
    {
        Assert.That(ReplicationConfigReport.Empty.Trees, Is.Empty);
    }

    [Test]
    public void ReplicationDisableResult_ctor_captures_state()
    {
        var result = new ReplicationDisableResult("orders", alreadyDisabled: true);

        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.AlreadyDisabled, Is.True);
    }

    [Test]
    public void ReplicationDisableResult_ctor_throws_for_null_tree()
        => Assert.That(() => new ReplicationDisableResult(null!, false), Throws.ArgumentNullException);

    [Test]
    public void ReplicationEnableResult_ctor_captures_state()
    {
        var result = new ReplicationEnableResult(
            "orders", LatticeMergeMode.OrSet, alreadyEnabled: true, bootstrapRequested: true);

        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.True);
    }

    [Test]
    public void ReplicationEnableResult_ctor_throws_for_null_tree()
        => Assert.That(() => new ReplicationEnableResult(null!, LatticeMergeMode.OrSet, false, false),
            Throws.ArgumentNullException);

    [Test]
    public void ReplicationTreeConfigEntry_ctor_captures_state()
    {
        var entry = new ReplicationTreeConfigEntry(
            "orders", enabled: true, mode: LatticeMergeMode.PnCounter, ambiguous: false);

        Assert.That(entry.TreeId, Is.EqualTo("orders"));
        Assert.That(entry.Enabled, Is.True);
        Assert.That(entry.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
        Assert.That(entry.Ambiguous, Is.False);
    }

    [Test]
    public void ReplicationTreeConfigEntry_ctor_accepts_null_mode()
    {
        var entry = new ReplicationTreeConfigEntry("orders", enabled: false, mode: null, ambiguous: true);

        Assert.That(entry.Mode, Is.Null);
        Assert.That(entry.Ambiguous, Is.True);
    }

    [Test]
    public void ReplicationTreeConfigEntry_ctor_throws_for_null_tree()
        => Assert.That(() => new ReplicationTreeConfigEntry(null!, false, null, false),
            Throws.ArgumentNullException);
}
