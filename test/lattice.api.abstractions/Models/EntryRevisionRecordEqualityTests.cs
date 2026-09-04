using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="EntryRevisionRecord"/>, the
/// abstraction-layer twin of the core <c>EntryRevision</c>. Its
/// <see cref="EntryRevisionRecord.ValuePreview"/> /
/// <see cref="EntryRevisionRecord.Delta"/> byte arrays were compared by reference
/// and its <see cref="EntryRevisionRecord.MemberChanges"/> list by reference under
/// the compiler-generated record equality, so two structurally identical revisions
/// - including a revision and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class EntryRevisionRecordEqualityTests
{
    private static CrdtMemberChange Change(byte[]? element = null) => new()
    {
        Element = element ?? [10, 11],
        Kind = CrdtMemberChangeKind.Added,
        ReplicaId = "r1",
        Ordinal = 3,
    };

    private static EntryRevisionRecord Sample(
        byte[]? valuePreview = null,
        byte[]? delta = null,
        IReadOnlyList<CrdtMemberChange>? memberChanges = null) => new()
    {
        Hlc = new HybridLogicalClock { WallClockTicks = 77, Counter = 4 },
        Kind = HistoryRowKind.CrdtDelta,
        Category = MutationCategory.User,
        SourceKey = "k",
        OriginClusterId = "cluster-b",
        ValuePreview = valuePreview ?? [1, 2],
        ValueLength = 9,
        Truncated = true,
        ValueHash = -7,
        Delta = delta ?? [3, 4, 5],
        Mode = LatticeMergeMode.OrSet,
        MemberChanges = memberChanges ?? [Change()],
        Retention = new RevisionRetention { Mode = HistoryRetentionMode.Hybrid, ValueRetained = true },
        EndKey = "z",
    };

    [Test]
    public void Equal_across_distinct_arrays_and_member_change_lists()
    {
        var a = Sample([1, 2], [3, 4, 5], [Change([9, 9])]);
        var b = Sample([1, 2], [3, 4, 5], [Change([9, 9])]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.ValuePreview, b.ValuePreview), Is.False);
            Assert.That(ReferenceEquals(a.Delta, b.Delta), Is.False);
            Assert.That(ReferenceEquals(a.MemberChanges, b.MemberChanges), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_preview_bytes_differ()
    {
        var a = Sample();
        var b = a with { ValuePreview = [9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_delta_bytes_differ()
    {
        var a = Sample();
        var b = a with { Delta = [9, 9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_member_change_content_differs()
    {
        var a = Sample(memberChanges: [Change([1])]);
        var b = Sample(memberChanges: [Change([2])]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_member_change_count_differs()
    {
        var a = Sample(memberChanges: [Change()]);
        var b = Sample(memberChanges: [Change(), Change([5, 5])]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { SourceKey = "other" }), Is.False);
            Assert.That(a.Equals(a with { Category = MutationCategory.Maintenance }), Is.False);
            Assert.That(a.Equals(a with { ValueLength = 10 }), Is.False);
            Assert.That(a.Equals(a with { Kind = HistoryRowKind.Set }), Is.False);
        });
    }

    [Test]
    public void Equal_when_arrays_null_and_member_changes_empty_on_both_sides()
    {
        var a = Sample(memberChanges: []) with { ValuePreview = null, Delta = null };
        var b = Sample(memberChanges: []) with { ValuePreview = null, Delta = null };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_array_is_null()
    {
        var a = Sample();
        var b = a with { Delta = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var revision = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<EntryRevisionRecord>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(revision));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.ValuePreview, revision.ValuePreview), Is.False);
            Assert.That(decoded.Equals(revision), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(revision.GetHashCode()));
        });
    }
}
