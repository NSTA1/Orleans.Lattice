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
/// The split is deliberate and is the whole point of the fixture. Blanket
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
/// <c>GetAllTreeIdsAsync</c> is the third case and has its own guard below: it
/// is read-only but is deliberately <em>not</em> marked, because it is a range
/// traversal of the registry's own backing tree rather than a point lookup and
/// the system-tree scan path omits the topology re-probes that would keep it
/// correct under concurrent mutation. Marking it is what regressed
/// <c>Restore_reconciles_large_tag_membership_under_concurrent_reads</c>.
/// </para>
/// <para>
/// The member lists below are exhaustive and are asserted to be, so adding a
/// member to <see cref="ILatticeRegistry"/> fails this fixture until it is
/// classified. That is intentional: the classification
/// is a design decision about shared-state safety, not a detail to be inferred.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeRegistryInterleaveContractTests
{
    /// <summary>
    /// Read-only members that resolve a bounded set of <em>named</em> entries
    /// and mutate nothing, so admitting one mid-turn cannot tear any state: an
    /// entry is rewritten by exactly one terminal <c>SetAsync</c> against the
    /// backing tree, so a reader observes it wholly before or wholly after.
    /// </summary>
    private static readonly string[] PointReadMembers =
    [
        nameof(ILatticeRegistry.ExistsAsync),
        nameof(ILatticeRegistry.GetEntryAsync),
        nameof(ILatticeRegistry.GetEntriesAsync),
        nameof(ILatticeRegistry.ResolveAsync),
        nameof(ILatticeRegistry.GetShardMapAsync),
        nameof(ILatticeRegistry.GetWalPlacementAsync),
    ];

    /// <summary>
    /// Read-only members that are nonetheless excluded from interleaving because
    /// they scan a key range of the registry's own backing tree across many
    /// awaits rather than reading named entries.
    /// </summary>
    private static readonly string[] ScanMembers =
    [
        nameof(ILatticeRegistry.GetAllTreeIdsAsync),
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
    /// Every point read must interleave: these are what option resolution calls,
    /// and they are what must be able to overtake a long enumeration.
    /// </summary>
    [Test]
    public void Read_only_registry_members_are_marked_AlwaysInterleave(
        [ValueSource(nameof(PointReadMembers))] string methodName)
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
    /// The range-scan reads must NOT interleave, despite being read-only. This
    /// is the guard the original #3180 fix lacked, and its absence cost a real
    /// regression: marking <c>GetAllTreeIdsAsync</c> let a concurrent
    /// registration reshape the registry's backing tree under an in-flight
    /// cursor, so the scan silently dropped an already-registered id. That
    /// surfaced as a tag-index reconcile that never fired, because
    /// <c>TagIndexReconcileTrigger</c> discovers index trees through exactly
    /// this enumeration and a missing id is indistinguishable from no index.
    /// <para>
    /// Excluding it does not reintroduce the head-of-line block:
    /// <c>[AlwaysInterleave]</c> admits the <em>incoming</em> call past whatever
    /// is already running, so the point reads above overtake a running
    /// enumeration whether or not the enumeration is itself marked.
    /// </para>
    /// </summary>
    [Test]
    public void Range_scan_registry_members_are_not_marked_AlwaysInterleave(
        [ValueSource(nameof(ScanMembers))] string methodName)
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
                $"ILatticeRegistry.{Describe(overload)} scans a key range of the registry's own backing " +
                "tree across many awaits, and the system-tree scan path deliberately omits the topology " +
                "re-probes that would re-enter this grain. Those omissions are sound only while no " +
                "mutator can run during the scan, so marking this [AlwaysInterleave] lets a concurrent " +
                "registration drop an unrelated id from the result - a wrong answer, not a slow one. " +
                "It is also unnecessary: marking the point reads is what lets them overtake this scan.");
        }
    }

    /// <summary>
    /// The three lists above must together account for every member, so a new
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

        var classified = PointReadMembers
            .Concat(ScanMembers)
            .Concat(MutatingMembers)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(declared, Is.EquivalentTo(classified),
            "Every ILatticeRegistry member must be classified as a point read, a range scan, or a " +
            "mutator in this fixture. A new member is neither safe nor unsafe by default: decide " +
            "whether it can be admitted while another turn is mid-flight, then add it to the " +
            "matching list.");
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
