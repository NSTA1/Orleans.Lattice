using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="SpanAdmissionMigrationModel"/> run applies declared-span
/// admission to a cross-shard migration import, mirroring the two shapes the
/// production predicate at <c>BPlusLeafGrain.MergeManyAsync</c> can take.
/// </summary>
public enum SpanAdmissionMode
{
    /// <summary>
    /// The shipping pre-fix shape, <c>if (!isCrossShardMigration &amp;&amp;
    /// ContainsOutOfSpanKey(entries))</c>: a migration import skips declared-span
    /// admission entirely and is committed by whichever leaf it was addressed to,
    /// whether or not that leaf still declares the key.
    /// </summary>
    MigrationExempt,

    /// <summary>
    /// The fixed shape, <c>if (ContainsOutOfSpanKey(entries))</c>: a migration
    /// import is admitted by the same declared-span rule as every other write, so
    /// an import addressed to a leaf that no longer declares the key is forwarded
    /// to the leaf that does.
    /// </summary>
    AdmissionApplied,

    /// <summary>
    /// <see cref="AdmissionApplied"/>, but the safety property is replaced by a
    /// reachability witness asserting the dangerous ordering is never reached.
    /// Exploration is expected to violate it, which is what proves the fixed arm's
    /// silence is earned rather than an artefact of a step scheduler that never
    /// places the late import after the split. It is the counterpart of
    /// <see cref="ReshardGuardMode.NoReadGuard"/> in
    /// <see cref="ReshardMigrationModel"/>, and it asserts nothing about safety.
    /// </summary>
    AdmissionAppliedOrderingProbe,
}

/// <summary>
/// The leaf shape a cross-shard migration import is addressed to.
/// </summary>
public enum ImportDestination
{
    /// <summary>
    /// The harmful shape: a leaf whose declared range was <b>narrowed by its own
    /// split</b>, receiving a late import for a key that split had already moved
    /// to the sibling.
    /// </summary>
    SplitNarrowedDonor,

    /// <summary>
    /// The legitimate topology-seeding shape in which the import can arrive before
    /// the coordinator sets the destination's range. Both bounds are null, so
    /// <c>HasDeclaredSpan</c> is false and admission returns without a comparison.
    /// </summary>
    UnboundedFreshDestination,

    /// <summary>
    /// The legitimate topology-seeding shape in which the coordinator has set a
    /// range that does not yet cover the imported key, and the destination has no
    /// chain pointers. Admission diverts, resolves no forward target, and
    /// <c>ForwardOutOfSpanMergeAsync</c> falls open to a local commit.
    /// </summary>
    ChainlessFreshDestination,
}

/// <summary>
/// A Coyote concurrency model of the <b>declared-span admission exemption</b> for
/// cross-shard migration imports: the <c>!isCrossShardMigration</c> term that let a
/// migration import bypass the rule that a leaf only commits a key its own
/// <c>[LowKeyInclusive, HighKeyExclusive)</c> range covers.
/// <para>
/// The exemption was justified by topology seeding, and that premise is true: a
/// coordinator does place rows before setting the destination's range. But the
/// seeding shape is admitted anyway, by two independent properties of the admission
/// path that this model exercises directly - a destination with two null bounds
/// never enters the scan at all, and a destination with no chain pointers resolves
/// no forward target and falls open to a local commit. The only shape the exemption
/// actually changed is therefore the harmful one: a leaf whose span was narrowed by
/// its own split, accepting a late import for a key that split had already moved
/// away, leaving a <b>stale resurrected row on a leaf that does not declare the
/// key</b>.
/// </para>
/// <para>
/// <b>Atomicity granularity.</b> The span-narrowing step, the foreground commit, and
/// the late import's arrival are three independently schedulable steps that fire
/// exactly once each in a runtime-chosen order, so all six permutations are
/// reachable. This is deliberate and is the property the model lives or dies by:
/// <see cref="ReshardMigrationModel"/> cannot express this defect because its
/// migration preamble contains no controlled choice and its value and marker
/// mutations are fused into one indivisible step, so the interleaving that produces
/// the bug is absent from its state space rather than absent from its assertions.
/// A model whose steps are fused is not conservative, it is silently blind while
/// still reporting a proof.
/// </para>
/// <para>
/// Admission executes the real <see cref="SplitBoundary.Owns"/> rule - the same
/// call <c>ShouldApplyDuringReplay</c>, <c>DeclaresKey</c> and
/// <c>TryResolveSpanForwardTarget</c> make - so the ownership half of the model
/// cannot drift from production. The forward-target resolution and the asymmetric
/// migration guard are transcriptions of
/// <c>BPlusLeafGrain.SpanAdmission.cs</c> and <c>BPlusLeafGrain.cs</c> respectively.
/// </para>
/// </summary>
public sealed class SpanAdmissionMigrationModel : ICoyoteModel
{
    /// <summary>The imported key, and the split key, so the donor is sealed exactly at it.</summary>
    private const string Key = "k50";

    /// <summary>The donor's inclusive low bound before and after the split.</summary>
    private const string DonorLow = "k00";

    /// <summary>
    /// A seeded high bound that deliberately excludes <see cref="Key"/>, used by
    /// <see cref="ImportDestination.ChainlessFreshDestination"/> to force admission
    /// to divert so the fail-open path is the thing under test.
    /// </summary>
    private const string ShortHigh = "k10";

    /// <summary>The leaf holds no row for the key.</summary>
    private const int None = 0;

    /// <summary>The value the key held before the split, and the value the late import carries.</summary>
    private const int Pre = 1;

    /// <summary>The authoritative post-split value, committed by a foreground write.</summary>
    private const int Post = 2;

    private const int StepNarrow = 0;
    private const int StepForeground = 1;
    private const int StepImport = 2;
    private const int DonorStepCount = 3;

    private const int StepSeedRange = 0;
    private const int SeedStepCount = 2;

    private const int Donor = 0;
    private const int Sibling = 1;
    private const int NoLeaf = -1;

    private readonly SpanAdmissionMode _mode;
    private readonly ImportDestination _destination;

    /// <summary>
    /// Creates the model for an import addressed to a leaf of the given
    /// <paramref name="destination"/> shape, under the given admission
    /// <paramref name="mode"/>.
    /// </summary>
    public SpanAdmissionMigrationModel(SpanAdmissionMode mode, ImportDestination destination)
    {
        _mode = mode;
        _destination = destination;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        if (_destination == ImportDestination.SplitNarrowedDonor)
        {
            RunSplitNarrowedDonor(runtime);
        }
        else
        {
            RunFreshDestination(runtime);
        }
    }

    /// <summary>
    /// The harmful shape. A donor holding the key splits, a foreground write commits
    /// the authoritative value to whichever leaf then declares the key, and a late
    /// migration import carrying the pre-split value arrives at the donor. The three
    /// steps are ordered by the runtime.
    /// </summary>
    private void RunSplitNarrowedDonor(ICoyoteRuntime runtime)
    {
        var leaves = new Leaf[2];
        leaves[Donor] = new Leaf
        {
            Present = true,
            Low = DonorLow,
            High = null,
            Next = NoLeaf,
            Prev = NoLeaf,
            Value = Pre,
            IsMigrated = false,
        };

        var done = new bool[DonorStepCount];
        var narrowed = false;
        var importAfterNarrow = false;

        for (var fired = 0; fired < DonorStepCount; fired++)
        {
            var step = ChooseNextStep(runtime, done);
            done[step] = true;

            switch (step)
            {
                case StepNarrow:
                    Narrow(leaves);
                    narrowed = true;
                    break;

                case StepForeground:
                    CommitForeground(leaves);
                    break;

                default:
                    importAfterNarrow = narrowed;
                    DeliverImport(leaves, Donor);
                    break;
            }
        }

        if (_mode == SpanAdmissionMode.AdmissionAppliedOrderingProbe)
        {
            AssertLateImportOrderingUnreachable(importAfterNarrow);
            return;
        }

        AssertSingleDeclaringHome(leaves);
    }

    /// <summary>
    /// The legitimate topology-seeding shapes. The coordinator's range-set step and
    /// the import are independently ordered, so the import can land before or after
    /// the destination declares anything. Either way the seed must come to rest on
    /// the destination.
    /// </summary>
    private void RunFreshDestination(ICoyoteRuntime runtime)
    {
        var leaves = new Leaf[2];
        leaves[Donor] = new Leaf
        {
            Present = true,
            Low = null,
            High = null,
            Next = NoLeaf,
            Prev = NoLeaf,
            Value = None,
            IsMigrated = false,
        };

        var done = new bool[SeedStepCount];

        for (var fired = 0; fired < SeedStepCount; fired++)
        {
            var step = ChooseNextStep(runtime, done);
            done[step] = true;

            if (step == StepSeedRange)
            {
                SeedRange(leaves);
            }
            else
            {
                DeliverImport(leaves, Donor);
            }
        }

        AssertSeedLandedLocally(leaves);
    }

    /// <summary>
    /// The donor's own split, as <c>CompleteSplit</c> performs it inside the donor's
    /// turn: the high bound narrows to the split key, the successor pointer is
    /// installed, the sibling comes into being declaring the upper half, and the
    /// donor's row for the key moves across. After this step the donor no longer
    /// declares the key.
    /// </summary>
    private static void Narrow(Leaf[] leaves)
    {
        leaves[Sibling] = new Leaf
        {
            Present = true,
            Low = Key,
            High = null,
            Next = NoLeaf,
            Prev = Donor,
            Value = leaves[Donor].Value,
            IsMigrated = leaves[Donor].IsMigrated,
        };

        leaves[Donor].High = Key;
        leaves[Donor].Next = Sibling;
        leaves[Donor].Value = None;
        leaves[Donor].IsMigrated = false;
    }

    /// <summary>
    /// A foreground write of the authoritative value. It routes to whichever present
    /// leaf declares the key, so before the split it lands on the donor and after it
    /// lands on the sibling, exactly as separator descent would take it.
    /// </summary>
    private static void CommitForeground(Leaf[] leaves)
    {
        for (var i = 0; i < leaves.Length; i++)
        {
            if (leaves[i].Present && SplitBoundary.Owns(Key, leaves[i].Low, leaves[i].High))
            {
                leaves[i].Value = Post;
                leaves[i].IsMigrated = false;
                return;
            }
        }
    }

    /// <summary>
    /// The coordinator's separate range-set step for a freshly created destination.
    /// <see cref="ImportDestination.ChainlessFreshDestination"/> seeds a range that
    /// does not cover the key, which is what forces admission to divert and puts the
    /// fail-open rule under test rather than the null-bounds short circuit.
    /// </summary>
    private void SeedRange(Leaf[] leaves)
    {
        leaves[Donor].Low = DonorLow;
        leaves[Donor].High = _destination == ImportDestination.ChainlessFreshDestination ? ShortHigh : null;
    }

    /// <summary>
    /// Delivers the cross-shard migration import to leaf <paramref name="index"/>,
    /// executing the two production decisions in order: declared-span admission at
    /// <c>BPlusLeafGrain.MergeManyAsync</c>, then the asymmetric migration guard in
    /// <c>MergeIntoStateAsync</c>. The import is a migration by construction, so the
    /// <c>isCrossShardMigration</c> argument is constant-true here and is not
    /// modelled as a parameter.
    /// </summary>
    private void DeliverImport(Leaf[] leaves, int index)
    {
        // BPlusLeafGrain.cs: if (!isCrossShardMigration && ContainsOutOfSpanKey(entries)).
        // Under MigrationExempt the whole admission step is skipped for a migration
        // import, which is the clause under test.
        if (_mode != SpanAdmissionMode.MigrationExempt && ContainsOutOfSpanKey(leaves, index))
        {
            if (TryResolveSpanForwardTarget(leaves, index, out var target))
            {
                DeliverImport(leaves, target);
                return;
            }

            // Fail-open, deliberately: an out-of-span key with no resolvable target
            // is committed locally rather than failing the write.
        }

        // BPlusLeafGrain.MergeIntoStateAsync: a migration import must not overwrite an
        // existing non-migrated entry, whose HLC can be dominated by the source leaf's
        // clock even though it is logically newer. Note this guard requires the row to
        // be PRESENT; the split has already removed it from the donor, so on the donor
        // there is nothing for it to hit and it cannot suppress the resurrection.
        if (leaves[index].Value != None && !leaves[index].IsMigrated)
        {
            return;
        }

        leaves[index].Value = Pre;
        leaves[index].IsMigrated = true;
    }

    /// <summary>
    /// <c>BPlusLeafGrain.SpanAdmission.cs</c>'s <c>ContainsOutOfSpanKey</c> for the
    /// single modelled key: a leaf with two null bounds declares the whole keyspace,
    /// so no key can be out of span and the scan is skipped outright.
    /// </summary>
    private static bool ContainsOutOfSpanKey(Leaf[] leaves, int index)
    {
        var hasDeclaredSpan = leaves[index].Low is not null || leaves[index].High is not null;
        return hasDeclaredSpan && !SplitBoundary.Owns(Key, leaves[index].Low, leaves[index].High);
    }

    /// <summary>
    /// <c>BPlusLeafGrain.SpanAdmission.cs</c>'s <c>TryResolveSpanForwardTarget</c>: a
    /// key at or above the high bound goes to the successor, a key below the low
    /// bound to the predecessor, and a missing or self-referential pointer resolves
    /// nothing so the caller falls open to a local commit.
    /// </summary>
    private static bool TryResolveSpanForwardTarget(Leaf[] leaves, int index, out int target)
    {
        target = NoLeaf;

        var low = leaves[index].Low;
        var high = leaves[index].High;
        if (SplitBoundary.Owns(Key, low, high))
        {
            return false;
        }

        var candidate = high is not null && string.CompareOrdinal(Key, high) >= 0
            ? leaves[index].Next
            : leaves[index].Prev;

        if (candidate == NoLeaf || candidate == index || !leaves[candidate].Present)
        {
            return false;
        }

        target = candidate;
        return true;
    }

    /// <summary>
    /// Picks the next step to fire from those not yet fired, driving the choice
    /// through the runtime so exploration enumerates every ordering. The last
    /// remaining candidate is taken when no coin comes up, which keeps every run
    /// total - all steps always fire, only their order varies.
    /// </summary>
    private static int ChooseNextStep(ICoyoteRuntime runtime, bool[] done)
    {
        var last = NoLeaf;
        for (var step = 0; step < done.Length; step++)
        {
            if (done[step])
            {
                continue;
            }

            last = step;
            if (runtime.RandomBoolean())
            {
                return step;
            }
        }

        return last;
    }

    /// <summary>
    /// The safety property: after the split, the key must be held by exactly one
    /// leaf, that leaf must be the one that declares it, and its value must be the
    /// authoritative post-split value. A second holder is the stale resurrected row -
    /// invisible to a reader that routes correctly, but served to a reader routed to
    /// the old leaf and returned by any scan that visits it.
    /// </summary>
    private static void AssertSingleDeclaringHome(Leaf[] leaves)
    {
        var holders = 0;
        var holder = NoLeaf;
        for (var i = 0; i < leaves.Length; i++)
        {
            if (leaves[i].Present && leaves[i].Value != None)
            {
                holders++;
                holder = i;
            }
        }

        // The message is built only on the failing path so exploration does not pay
        // for string formatting on every one of a thousand passing runs.
        if (holders != 1)
        {
            Specification.Assert(
                false,
                $"{holders} leaves hold key {Key} after a late migration import: " +
                $"donor={leaves[Donor].Value} (migrated={leaves[Donor].IsMigrated}), " +
                $"sibling={leaves[Sibling].Value} (migrated={leaves[Sibling].IsMigrated}). " +
                "The donor no longer declares the key, so its row is a stale resurrection " +
                "that a scan visiting the donor will serve.");
            return;
        }

        if (!SplitBoundary.Owns(Key, leaves[holder].Low, leaves[holder].High))
        {
            Specification.Assert(
                false,
                $"key {Key} came to rest on a leaf that does not declare it " +
                $"([{leaves[holder].Low ?? "-inf"}, {leaves[holder].High ?? "+inf"})), " +
                $"holding value {leaves[holder].Value}.");
            return;
        }

        if (leaves[holder].Value != Post)
        {
            Specification.Assert(
                false,
                $"key {Key} resolved to {leaves[holder].Value} but the authoritative " +
                $"post-split value is {Post}; a late migration import overwrote the " +
                "foreground commit.");
        }
    }

    /// <summary>
    /// The no-regression property for topology seeding: the imported row must come to
    /// rest on the destination it was addressed to, and must not have been forwarded
    /// away, whichever order the coordinator's range-set step and the import land in.
    /// </summary>
    private static void AssertSeedLandedLocally(Leaf[] leaves)
    {
        if (leaves[Donor].Value != Pre)
        {
            Specification.Assert(
                false,
                $"a migration seed for key {Key} did not land on its destination: " +
                $"destination holds {leaves[Donor].Value}, expected {Pre}. " +
                "Declared-span admission diverted a legitimate topology-seeding import.");
            return;
        }

        if (leaves[Sibling].Present && leaves[Sibling].Value != None)
        {
            Specification.Assert(
                false,
                $"a migration seed for key {Key} was forwarded off its destination and " +
                $"came to rest elsewhere holding {leaves[Sibling].Value}.");
        }
    }

    /// <summary>
    /// The reachability witness used by
    /// <see cref="SpanAdmissionMode.AdmissionAppliedOrderingProbe"/>. It asserts the
    /// dangerous ordering is never reached, so exploration finding a violation is the
    /// proof that it is - and therefore that the fixed arm's silence is a property of
    /// the fix rather than of a scheduler that never tried.
    /// </summary>
    private static void AssertLateImportOrderingUnreachable(bool importAfterNarrow)
    {
        if (importAfterNarrow)
        {
            Specification.Assert(
                false,
                "reachability witness: a late migration import was scheduled after the " +
                "donor's span narrowed, which is the ordering in which the exemption " +
                "changes the outcome.");
        }
    }

    /// <summary>
    /// One modelled leaf: its declared half-open range, its chain pointers, and the
    /// single modelled key's value and migration flag. A mutable struct held in an
    /// array so a step mutates it in place without allocating.
    /// </summary>
    private struct Leaf
    {
        /// <summary>Whether this leaf exists yet; the sibling appears only at the split.</summary>
        public bool Present;

        /// <summary>The inclusive low bound, or null for no lower constraint.</summary>
        public string? Low;

        /// <summary>The exclusive high bound, or null for no upper constraint.</summary>
        public string? High;

        /// <summary>The successor leaf index, or <see cref="NoLeaf"/>.</summary>
        public int Next;

        /// <summary>The predecessor leaf index, or <see cref="NoLeaf"/>.</summary>
        public int Prev;

        /// <summary>The value held for the modelled key, or <see cref="None"/>.</summary>
        public int Value;

        /// <summary>Whether the held row arrived as a cross-shard migration import.</summary>
        public bool IsMigrated;
    }
}
