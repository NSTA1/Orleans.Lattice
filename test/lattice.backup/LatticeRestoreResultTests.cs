using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeRestoreResult"/> focused on the tenant
/// dead-letter counters (<see cref="LatticeRestoreResult.DeadLetteredCrossTenant"/>
/// and <see cref="LatticeRestoreResult.DeadLetteredOverQuota"/>): they default to
/// zero on the tenancy-off path, carry the supplied non-negative counts otherwise,
/// and reject a negative count. The core required-argument guards are covered too.
/// </summary>
[TestFixture]
public sealed class LatticeRestoreResultTests
{
    private static readonly IReadOnlyList<string> Chain = ["base"];

    private static LatticeRestoreResult Create(
        long deadLetteredCrossTenant = 0,
        long deadLetteredOverQuota = 0) =>
        new(
            "backup-1",
            "t/acme/orders",
            LatticeRestoreMode.InPlace,
            "op-1",
            Chain,
            entriesApplied: 5,
            deadLetteredCrossTenant: deadLetteredCrossTenant,
            deadLetteredOverQuota: deadLetteredOverQuota);

    [Test]
    public void Dead_letter_counters_default_to_zero()
    {
        var result = Create();

        Assert.Multiple(() =>
        {
            Assert.That(result.DeadLetteredCrossTenant, Is.Zero);
            Assert.That(result.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public void Dead_letter_counters_carry_the_supplied_counts()
    {
        var result = Create(deadLetteredCrossTenant: 3, deadLetteredOverQuota: 7);

        Assert.Multiple(() =>
        {
            Assert.That(result.DeadLetteredCrossTenant, Is.EqualTo(3));
            Assert.That(result.DeadLetteredOverQuota, Is.EqualTo(7));
            Assert.That(result.EntriesApplied, Is.EqualTo(5));
        });
    }

    [Test]
    public void Negative_cross_tenant_count_throws()
    {
        Assert.That(() => Create(deadLetteredCrossTenant: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Negative_over_quota_count_throws()
    {
        Assert.That(() => Create(deadLetteredOverQuota: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Required_string_arguments_are_guarded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeRestoreResult("", "t", LatticeRestoreMode.InPlace, "op", Chain, 0),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => new LatticeRestoreResult("b", "", LatticeRestoreMode.InPlace, "op", Chain, 0),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => new LatticeRestoreResult("b", "t", LatticeRestoreMode.InPlace, "", Chain, 0),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => new LatticeRestoreResult("b", "t", LatticeRestoreMode.InPlace, "op", null!, 0),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeRestoreResult("b", "t", LatticeRestoreMode.InPlace, "op", Chain, -1),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
