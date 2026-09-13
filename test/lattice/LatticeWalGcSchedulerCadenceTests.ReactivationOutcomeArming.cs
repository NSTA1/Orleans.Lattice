using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Proves the reactivation outcome enum is <b>exhaustively armed</b> (issue
/// #2938), which is a stronger property than the one the priming fixture
/// establishes and is not implied by it.
/// <para>
/// Priming proves a detector exists for the arms that have one. It says nothing
/// about an arm with no detector at all - and an outcome with no instrument
/// reports a structural zero that is indistinguishable from a measured one,
/// while passing every priming audit. That was the live state of this
/// instrument: three of its four terminal outcomes were unarmed, the HELP text
/// assured readers its zeros were measured, and the uncounted outcome fired 234
/// times in a single observation window.
/// </para>
/// <para>
/// The check here is deliberately <b>one-directional</b>: every terminal
/// outcome must have an arm, but not every arm need be a terminal outcome. The
/// instrument's tag space is a union - <c>attempted</c>, <c>healed</c>,
/// <c>abandoned</c> and <c>rearmed</c> are lifecycle events rather than members
/// of the enum - so a symmetric check would reject four legitimate arms on its
/// first run.
/// </para>
/// <para>
/// <b>Why this gate cannot go vacuous by rename.</b> A reflection gate that
/// resolves its target by name turns into a green no-op the moment the target
/// is renamed or moved. This gate is bound to the enum at <b>compile time</b>
/// through <see cref="Enum.GetValues{TEnum}"/>, so a rename is a build error
/// rather than a silent pass - the vacuity route is closed by construction, not
/// by an assertion that could itself be deleted. Do not "improve" this into a
/// name-based reflection scan; that would trade a compile-time guarantee for a
/// runtime one that fails open. The two anti-vacuity assertions below guard the
/// remaining route, which is a mapping or an enum that is reachable but empty.
/// </para>
/// <para>
/// <b>Known boundary - deliberately not fixed here.</b> A one-directional
/// enum-to-arm gate says nothing about the four lifecycle arms. If
/// <c>healed</c> were dropped from the recording path tomorrow, nothing in this
/// fixture would catch it, because <c>healed</c> is not an enum member and this
/// gate only walks the enum. That is an accepted limitation of the narrow gate
/// rather than an oversight; the general form, which would need a marker
/// attribute to relate arms to their source without re-introducing the arity
/// mismatch above, is tracked as issue #2939. It is written down here so a
/// later reader does not mistake this gate for coverage of the whole tag space.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>The terminal arms, which partition every attempted touch.</summary>
    private static readonly string[] TerminalOutcomeArms =
        ["completed", "unresolvable", "faulted", "undelivered"];

    [Test]
    public void ReactivationOutcomeTag_arms_every_declared_terminal_outcome()
    {
        // The member list is taken from the enum itself rather than from a copy,
        // which is the only source that cannot be missing the member this gate
        // exists to catch. A list maintained beside the enum would omit a new
        // member for exactly the same reason the arm was omitted.
        var members = Enum.GetValues<LatticeWalGcScheduler.ReactivationOutcome>();

        // Anti-vacuity, input side. A gate that silently enumerated nothing would
        // pass for the whole life of the defect it is meant to prevent, which is
        // the failure mode this epic has now found inside three passing gates.
        Assert.That(members, Has.Length.GreaterThanOrEqualTo(4),
            "the enum must actually have members, or the arming assertion below proves nothing.");

        // Calling the mapping for every member IS the exhaustiveness check: an
        // unmapped member throws rather than returning a neighbouring bucket.
        var armed = members
            .Select(m => LatticeWalGcScheduler.ReactivationOutcomeTag(m).Value as string)
            .ToArray();

        // Anti-vacuity, scanned side. Asserted separately from the member count
        // because the two can diverge: a mapping that returned nothing for every
        // member would leave the count above satisfied and compare an empty set
        // against an empty set.
        Assert.That(armed, Is.Not.Empty,
            "the mapping must have produced arms, or the comparison below is empty-against-empty.");

        Assert.Multiple(() =>
        {
            Assert.That(armed, Is.Unique,
                "two outcomes sharing an arm would sum into one series and could never be told apart.");
            Assert.That(armed, Has.None.Null);
            Assert.That(armed, Is.EquivalentTo(TerminalOutcomeArms),
                "the armed set must match the documented terminal arms exactly, so a rename cannot silently orphan a dashboard or a query.");
        });
    }

    [Test]
    public void ReactivationOutcome_refund_classes_partition_the_enum()
    {
        // Every member must fall on one side of the refund split, and both sides
        // must be occupied. A member added without being classified would take
        // the default and silently join the unrefunded class, changing its cost
        // per abandonment from 6 to 3 - which is the exact quantity the epic's
        // production diagnosis reads.
        var members = Enum.GetValues<LatticeWalGcScheduler.ReactivationOutcome>();
        var refunded = members.Where(LatticeWalGcScheduler.IsRefundableReactivationOutcome).ToArray();
        var charged = members.Where(m => !LatticeWalGcScheduler.IsRefundableReactivationOutcome(m)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(refunded.Length + charged.Length, Is.EqualTo(members.Length));
            Assert.That(refunded, Is.Not.Empty,
                "with nothing refunded the cap and the backoff would be unreachable.");
            Assert.That(charged, Is.Not.Empty,
                "with everything refunded no leaf would ever be abandoned, which is the permanent-retry failure the cap exists to stop.");
        });
    }

    /// <summary>
    /// Runs one permanently-blocked tree to its first abandonment and returns
    /// how many times each terminal arm fired.
    /// </summary>
    private static async Task<Dictionary<string, int>> TerminalArmCountsAsync(
        string treeId,
        Func<Task<string?>> probe,
        string? consumerId = null)
    {
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(time, probe, consumerId, treeId);

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);

            return TerminalOutcomeArms.ToDictionary(
                arm => arm,
                arm => Outcomes(recorder, arm));
        }
    }

    [Test]
    public async Task ExecuteAsync_records_each_terminal_outcome_on_its_own_arm_and_no_other()
    {
        // Every arm is asserted present on its own probe and absent on the other
        // three, which makes this a 4x4 identity matrix rather than four
        // independent assertions. That shape is what discharges #2938's own
        // standard: each off-diagonal zero is proven observable by the diagonal
        // entry in the same matrix, so no zero here is an unearned one. Four
        // separate fixtures would each have had to carry a positive control of
        // their own, and an absence with no control is precisely the defect
        // being fixed.
        var matrix = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal)
        {
            ["completed"] = await TerminalArmCountsAsync(
                "arming-completed", () => Task.FromResult<string?>("arming-completed")),
            ["unresolvable"] = await TerminalArmCountsAsync(
                "arming-unresolvable",
                () => Task.FromResult<string?>("arming-unresolvable"),
                consumerId: "not-a-materialiser-consumer-id"),
            ["faulted"] = await TerminalArmCountsAsync(
                "arming-faulted", () => throw new InvalidOperationException("silo refused the call")),
            ["undelivered"] = await TerminalArmCountsAsync(
                "arming-undelivered", () => throw new TimeoutException("silo busy")),
        };

        Assert.Multiple(() =>
        {
            foreach (var (probe, counts) in matrix)
            {
                foreach (var arm in TerminalOutcomeArms)
                {
                    if (string.Equals(arm, probe, StringComparison.Ordinal))
                    {
                        Assert.That(counts[arm], Is.GreaterThan(0),
                            $"a probe producing '{probe}' must advance the '{arm}' arm, or that arm's zeros elsewhere are structural rather than measured.");
                    }
                    else
                    {
                        Assert.That(counts[arm], Is.Zero,
                            $"a probe producing '{probe}' must not advance the '{arm}' arm, or the outcomes are not separable.");
                    }
                }
            }
        });
    }

    [Test]
    public async Task ExecuteAsync_counts_every_attempted_touch_on_exactly_one_terminal_arm()
    {
        // The partition property the HELP text now claims, asserted rather than
        // asserted-about. This is the check that would have caught the original
        // defect immediately: with three arms unmapped the terminal arms summed
        // to a fraction of 'attempted', and no fixture anywhere compared the two.
        foreach (var (treeId, probe, consumerId) in new (string, Func<Task<string?>>, string?)[]
        {
            ("sum-completed", () => Task.FromResult<string?>("sum-completed"), null),
            ("sum-faulted", () => throw new InvalidOperationException("silo refused the call"), null),
            ("sum-undelivered", () => throw new TimeoutException("silo busy"), null),
            ("sum-unresolvable", () => Task.FromResult<string?>("sum-unresolvable"), "not-a-materialiser-consumer-id"),
        })
        {
            var time = new VirtualTimeProvider();
            var (scheduler, recorder) = BlockedTreeProbing(time, probe, consumerId, treeId);

            using (recorder)
            {
                await StartAndRunFirstPassAsync(scheduler, time);
                var attempted = await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
                var terminal = TerminalOutcomeArms.Sum(arm => Outcomes(recorder, arm));

                Assert.Multiple(() =>
                {
                    Assert.That(attempted, Is.GreaterThan(0),
                        $"'{treeId}' must issue touches, or its sum below is vacuously equal.");
                    Assert.That(terminal, Is.EqualTo(attempted),
                        $"'{treeId}' must account for every attempted touch on exactly one terminal arm, which is the partition the instrument's description promises.");
                });

                await scheduler.StopAsync(CancellationToken.None);
            }
        }
    }
}
