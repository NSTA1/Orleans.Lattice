using System.Collections.Immutable;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="WalPlacementAudit"/>, the read-surface record
/// returned by <see cref="ILatticeAdmin.AuditWalPlacementAsync"/>. It carried no
/// test at all, so none of its members were ever executed - which matters more
/// than it looks for a <c>[GenerateSerializer]</c> record: an operator reads the
/// drift verdict straight off these properties, and a value-equality bug in a
/// <c>readonly record struct</c> holding an <see cref="ImmutableArray{T}"/> is
/// silent until something compares two audits.
/// </summary>
[TestFixture]
public class WalPlacementAuditTests
{
    private static WalPartitionPlacement Partition(int index, string providerKey, bool resolvable) =>
        new() { Partition = index, ProviderKey = providerKey, ResolvableOnThisSilo = resolvable };

    private static WalPlacementAudit Audit(
        string treeId = "orders",
        long version = 7,
        bool allResolvable = true) => new()
        {
            TreeId = treeId,
            Version = version,
            PartitionCount = 2,
            Partitions = [Partition(0, "primary", true), Partition(1, "archive", allResolvable)],
            AllResolvableOnThisSilo = allResolvable,
            KnownProviderKeys = ["archive", "primary"],
        };

    [Test]
    public void Initialised_audit_round_trips_every_member()
    {
        var audit = Audit();

        Assert.Multiple(() =>
        {
            Assert.That(audit.TreeId, Is.EqualTo("orders"));
            Assert.That(audit.Version, Is.EqualTo(7));
            Assert.That(audit.PartitionCount, Is.EqualTo(2));
            Assert.That(audit.Partitions.Select(p => p.Partition), Is.EqualTo(new[] { 0, 1 }));
            Assert.That(audit.Partitions.Select(p => p.ProviderKey), Is.EqualTo(new[] { "primary", "archive" }));
            Assert.That(audit.AllResolvableOnThisSilo, Is.True);
            Assert.That(audit.KnownProviderKeys, Is.EqualTo(new[] { "archive", "primary" }));
        });
    }

    [Test]
    public void An_unresolvable_partition_is_reported_both_per_partition_and_in_the_rollup()
    {
        // The whole point of the audit: an operator must be able to see WHICH
        // partition drifted, not only that something did.
        var audit = Audit(allResolvable: false);

        Assert.Multiple(() =>
        {
            Assert.That(audit.AllResolvableOnThisSilo, Is.False);
            Assert.That(audit.Partitions.Where(p => !p.ResolvableOnThisSilo).Select(p => p.Partition),
                Is.EqualTo(new[] { 1 }));
        });
    }

    [Test]
    public void Default_audit_has_a_default_partition_array()
    {
        // The parameterless default is what a deserialiser produces for an
        // absent record; ImmutableArray's default is not an empty array, so
        // consumers must not assume it is safe to enumerate.
        var audit = default(WalPlacementAudit);

        Assert.Multiple(() =>
        {
            Assert.That(audit.TreeId, Is.Null);
            Assert.That(audit.Version, Is.Zero);
            Assert.That(audit.PartitionCount, Is.Zero);
            Assert.That(audit.Partitions.IsDefault, Is.True);
            Assert.That(audit.KnownProviderKeys.IsDefault, Is.True);
            Assert.That(audit.AllResolvableOnThisSilo, Is.False);
        });
    }

    [Test]
    public void Two_audits_over_the_same_partition_array_are_equal()
    {
        ImmutableArray<WalPartitionPlacement> partitions = [Partition(0, "primary", true)];
        ImmutableArray<string> keys = ["primary"];

        var a = new WalPlacementAudit
        {
            TreeId = "orders",
            Version = 3,
            PartitionCount = 1,
            Partitions = partitions,
            AllResolvableOnThisSilo = true,
            KnownProviderKeys = keys,
        };
        var b = a with { };

        Assert.Multiple(() =>
        {
            Assert.That(b, Is.EqualTo(a));
            Assert.That(b.GetHashCode(), Is.EqualTo(a.GetHashCode()));
        });
    }

    [Test]
    public void With_expression_replaces_only_the_named_member()
    {
        var audit = Audit();

        var bumped = audit with { Version = 8 };

        Assert.Multiple(() =>
        {
            Assert.That(bumped.Version, Is.EqualTo(8));
            Assert.That(bumped.TreeId, Is.EqualTo(audit.TreeId));
            Assert.That(bumped.Partitions, Is.EqualTo(audit.Partitions));
            Assert.That(bumped, Is.Not.EqualTo(audit));
        });
    }

    [Test]
    public void Audits_differing_only_in_the_resolvability_verdict_are_not_equal()
    {
        Assert.That(Audit(allResolvable: false), Is.Not.EqualTo(Audit(allResolvable: true)));
    }
}
