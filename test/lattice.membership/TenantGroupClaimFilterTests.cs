using System.Collections.ObjectModel;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the active <see cref="TenantGroupClaimFilter"/>: it reads its
/// activity from the supplied delegate and strips the whole reserved <c>t/</c>
/// namespace (well-formed tenant group ids, <c>t/default/...</c> and malformed
/// <c>t/...</c> ids alike) from every collection shape, leaving every other id.
/// </summary>
[TestFixture]
public sealed class TenantGroupClaimFilterTests
{
    private static readonly string[] Asserted =
    [
        "t/acme/admins",
        "cluster-readers",
        "t/default/admins",
        "t/BAD",
        "t/",
        "entra-engineering",
        "T/acme/admins",
        "tenant-x",
    ];

    private static readonly string[] Kept = ["cluster-readers", "entra-engineering", "T/acme/admins", "tenant-x"];

    [Test]
    public void Constructor_null_delegate_throws()
    {
        Assert.That(() => new TenantGroupClaimFilter(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void IsActive_reads_the_delegate_on_every_call()
    {
        var flag = false;
        var filter = new TenantGroupClaimFilter(() => flag);

        Assert.That(filter.IsActive, Is.False);
        flag = true;
        Assert.That(filter.IsActive, Is.True);
    }

    [Test]
    public void Filter_null_collection_throws()
    {
        var filter = new TenantGroupClaimFilter(static () => true);

        Assert.That(() => filter.Filter(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Filter_strips_the_reserved_namespace_from_a_list()
    {
        var groups = new List<string>(Asserted);

        new TenantGroupClaimFilter(static () => true).Filter(groups);

        Assert.That(groups, Is.EqualTo(Kept));
    }

    [Test]
    public void Filter_strips_the_reserved_namespace_from_a_hash_set()
    {
        var groups = new HashSet<string>(Asserted, StringComparer.Ordinal);

        new TenantGroupClaimFilter(static () => true).Filter(groups);

        Assert.That(groups, Is.EquivalentTo(Kept));
    }

    [Test]
    public void Filter_strips_every_occurrence_from_any_other_collection()
    {
        var groups = new Collection<string>(new List<string>(Asserted) { "t/acme/admins" });

        new TenantGroupClaimFilter(static () => true).Filter(groups);

        Assert.That(groups, Is.EqualTo(Kept));
    }

    [Test]
    public void Filter_leaves_a_collection_with_no_reserved_ids_untouched()
    {
        var groups = new Collection<string>(new List<string>(Kept));

        new TenantGroupClaimFilter(static () => true).Filter(groups);

        Assert.That(groups, Is.EqualTo(Kept));
    }

    [Test]
    public void Filter_empty_collection_is_a_no_op()
    {
        var groups = new List<string>();

        new TenantGroupClaimFilter(static () => true).Filter(groups);

        Assert.That(groups, Is.Empty);
    }

    [Test]
    public void IsActive_and_list_filtering_allocate_nothing_per_call()
    {
        var filter = new TenantGroupClaimFilter(static () => true);

        var growth = AllocationProbe.Growth(
            prepare: size =>
            {
                var lists = new List<string>[size];
                for (var i = 0; i < size; i++)
                {
                    lists[i] = new List<string>(Asserted);
                }

                return lists;
            },
            measure: (lists, size) =>
            {
                for (var i = 0; i < size; i++)
                {
                    if (filter.IsActive)
                    {
                        filter.Filter(lists[i]);
                    }

                    AllocationProbe.ScalarSink += lists[i].Count;
                }
            },
            smallSize: 16,
            largeSize: 256);

        Assert.That(growth, Is.Zero);
    }
}
