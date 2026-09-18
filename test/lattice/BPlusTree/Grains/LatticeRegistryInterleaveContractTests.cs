using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the concurrency contract of <see cref="ILatticeRegistry"/>: every
/// read-only member interleaves, every mutating member does not, and the grain
/// itself is never blanket <c>[Reentrant]</c>.
/// <para>
/// The registry is a process-wide singleton that every grain activation calls
/// during option resolution. Before issue #3180 no member interleaved, so
/// <c>GetAllTreeIdsAsync</c> - which fans out a whole-keyspace scan over the
/// registry's own backing system tree - held the singleton's only turn for the
/// length of that scan and head-of-line-blocked every unrelated option
/// resolution behind it. On the live deployment that presented as a climbing
/// <c>NonReentrancyQueueSize</c>, timeouts all targeting this one grain, and a
/// WAL GC scheduler that never completed a single registry enumeration.
/// </para>
/// <para>
/// The split is deliberate and is the whole point of the fixture: blanket
/// <c>[Reentrant]</c> would have fixed the block and silently voided the
/// read-modify-write atomicity that <c>ReassignSlotsAsync</c>,
/// <c>AllocateNextShardIndexAsync</c> and both <c>UpdateWalPlacementAsync</c>
/// overloads document as depending on non-reentrant scheduling. So this guard
/// asserts both directions - a missing attribute on a reader reintroduces the
/// block, and an added attribute on a mutator introduces silent lost updates in
/// split coordination and WAL placement, which is by far the worse defect and
/// the one nothing else here would catch.
/// </para>
/// <para>
/// The member lists below are exhaustive and are asserted to be, so adding a
/// member to <see cref="ILatticeRegistry"/> fails this fixture until it is
/// classified as a reader or a mutator. That is intentional: the classification
/// is a design decision about shared-state safety, not a detail to be inferred.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeRegistryInterleaveContractTests
{
    /// <summary>
    /// Members that only read registry state. Each resolves a single entry (or
    /// enumerates entries) and mutates nothing, so admitting one mid-turn
    /// cannot tear any state: an entry is rewritten by exactly one terminal
    /// <c>SetAsync</c> against the backing tree, so a reader observes it wholly
    /// before or wholly after.
    /// </summary>
    private static readonly string[] ReadOnlyMembers =
    [
        nameof(ILatticeRegistry.ExistsAsync),
        nameof(ILatticeRegistry.GetEntryAsync),
        nameof(ILatticeRegistry.GetEntriesAsync),
        nameof(ILatticeRegistry.GetAllTreeIdsAsync),
        nameof(ILatticeRegistry.ResolveAsync),
        nameof(ILatticeRegistry.GetShardMapAsync),
        nameof(ILatticeRegistry.GetWalPlacementAsync),
    ];

    /// <summary>
    /// Members that mutate shared registry state. These must stay
    /// non-interleaving so that the read-modify-write and compare-and-swap
    /// sequences inside them run to completion against every other mutator.
    /// </summary>
    private static readonly string[] MutatingMembers =
    [
        nameof(ILatticeRegistry.RegisterAsync),
        nameof(ILatticeRegistry.UpdateAsync),
        nameof(ILatticeRegistry.UnregisterAsync),
        nameof(ILatticeRegistry.SetAliasAsync),
        nameof(ILatticeRegistry.RemoveAliasAsync),
        nameof(ILatticeRegistry.SetShardMapAsync),
        nameof(ILatticeRegistry.ReassignSlotsAsync),
        nameof(ILatticeRegistry.AllocateNextShardIndexAsync),
        nameof(ILatticeRegistry.SetPublishEventsAsync),
        nameof(ILatticeRegistry.SetHistoryRetentionAsync),
        nameof(ILatticeRegistry.SetMaintainProjectionDigestAsync),
        nameof(ILatticeRegistry.SetMaxCacheValueBytesAsync),
        nameof(ILatticeRegistry.LatchProjectionDigestPermanentlyDisabledAsync),
        nameof(ILatticeRegistry.UpdateWalPlacementAsync),
    ];

    /// <summary>
    /// Every read-only member must interleave. Overloads are covered
    /// individually - <c>GetAllTreeIdsAsync</c> in particular has two, and the
    /// prefix overload is the one the WAL GC scheduler calls.
    /// </summary>
    [Test]
    public void Read_only_registry_members_are_marked_AlwaysInterleave(
        [ValueSource(nameof(ReadOnlyMembers))] string methodName)
    {
        var overloads = typeof(ILatticeRegistry)
            .GetMethods()
            .Where(m => m.Name == methodName)
            .ToArray();

        Assert.That(overloads, Is.Not.Empty,
            $"Expected to find method '{methodName}' on ILatticeRegistry. If it was renamed, update this " +
            "guard rather than deleting the entry - the classification is load-bearing.");

        foreach (var overload in overloads)
        {
            Assert.That(overload.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
                $"ILatticeRegistry.{Describe(overload)} must be [AlwaysInterleave]. The registry is a " +
                "process-wide singleton every grain activation calls during option resolution, so a " +
                "read that queues behind a long turn head-of-line-blocks the whole process (issue #3180).");
        }
    }

    /// <summary>
    /// No mutating member may interleave. This is the direction that fails
    /// silently in production: an interleaved mutator does not time out or
    /// throw, it loses an update.
    /// </summary>
    [Test]
    public void Mutating_registry_members_are_not_marked_AlwaysInterleave(
        [ValueSource(nameof(MutatingMembers))] string methodName)
    {
        var overloads = typeof(ILatticeRegistry)
            .GetMethods()
            .Where(m => m.Name == methodName)
            .ToArray();

        Assert.That(overloads, Is.Not.Empty,
            $"Expected to find method '{methodName}' on ILatticeRegistry.");

        foreach (var overload in overloads)
        {
            Assert.That(overload.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Null,
                $"ILatticeRegistry.{Describe(overload)} mutates shared registry state and must NOT be " +
                "[AlwaysInterleave]. ReassignSlotsAsync, AllocateNextShardIndexAsync and both " +
                "UpdateWalPlacementAsync overloads document non-reentrant scheduling as the mechanism " +
                "that makes their read-modify-write atomic; interleaving them loses split reassignments " +
                "and WAL placement moves with no error anywhere.");
        }
    }

    /// <summary>
    /// The two lists above must together account for every member, so a new
    /// member cannot be added without being classified. Without this the guard
    /// would go quietly incomplete rather than red.
    /// </summary>
    [Test]
    public void Every_registry_member_is_classified_by_this_guard()
    {
        var declared = typeof(ILatticeRegistry)
            .GetMethods()
            .Select(m => m.Name)
            .Distinct()
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(declared, Is.Not.Empty,
            "Reflection over ILatticeRegistry found no methods. The scan is broken, not the interface.");

        var classified = ReadOnlyMembers
            .Concat(MutatingMembers)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(declared, Is.EquivalentTo(classified),
            "Every ILatticeRegistry member must be classified as read-only or mutating in this fixture. " +
            "A new member is neither safe nor unsafe by default: decide whether it can be admitted while " +
            "another turn is mid-flight, then add it to the matching list.");
    }

    /// <summary>
    /// The grain must not be blanket <c>[Reentrant]</c>. That attribute would
    /// make the mutating members interleave too, defeating the split this
    /// fixture exists to hold, and it would do so without failing either guard
    /// above because the attribute lives on the class rather than the members.
    /// </summary>
    [Test]
    public void Registry_grain_is_not_blanket_Reentrant()
    {
        var attribute = typeof(LatticeRegistryGrain).GetCustomAttribute<ReentrantAttribute>(inherit: false);

        Assert.That(attribute, Is.Null,
            "LatticeRegistryGrain must not be [Reentrant]. Reentrancy applies to every member including " +
            "the read-modify-write mutators, whose atomicity depends on non-reentrant scheduling. The " +
            "head-of-line block in issue #3180 is fixed by [AlwaysInterleave] on the readers only.");
    }

    private static string Describe(MethodInfo method) =>
        $"{method.Name}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))})";
}
