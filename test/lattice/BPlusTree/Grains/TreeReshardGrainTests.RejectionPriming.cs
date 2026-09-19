using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2918: the <c>reason</c> taxonomy on
/// <c>orleans_lattice_shard_root_reshard_rejected_total</c> must be readable at
/// zero, not only at the one value that has already fired.
/// <para>
/// <b>What was wrong.</b> The counter declares a bounded six-member <c>reason</c>
/// domain and the only writes to it were the six increments. A cluster in which
/// no reshard had ever been refused for <c>shrink_unsupported</c> published no
/// such series at all, which scrapes identically to a build where that call site
/// was deleted, and identically again to a build where the whole counter is
/// unwired. So the reading an operator wants - "nothing has been refused for
/// this reason" - was not obtainable, and neither was the reading a reviewer
/// wants, "this rejection path is present in the build".
/// </para>
/// <para>
/// <b>Pushback on the issue's stated cause.</b> #2918 filed all three of its
/// instruments as unprimable because they are declared on a static metrics class
/// with no silo-startup hook. That premise is too pessimistic and is the reason
/// the work looked blocked. The instrument is <em>emitted</em> from a grain, and
/// the emitting grain's entry point is a lifecycle seam with all the
/// reachability a prime needs - the same mechanism issue #2809 already used for
/// the scan-page counters on <c>ShardRootGrain</c>. No new hook was required for
/// any of the three.
/// </para>
/// <para>
/// <b>Why the prime sits where it does.</b> All six rejection sites are inside
/// <c>ReshardAsync</c>, each behind its own early return or throw. A prime below
/// any one of them is unreachable on exactly the path whose absence it exists to
/// make readable, so it goes above all six. It sits deliberately BELOW the
/// internal-origin gate: a call refused for a non-internal origin never reaches
/// any of the six and is not a reshard rejection in this taxonomy, so the
/// population primed is exactly the population that can arm the counter.
/// </para>
/// </summary>
public partial class TreeReshardGrainTests
{
    /// <summary>
    /// The rejection reasons the grain arms, in the order the source declares
    /// them. Kept here rather than read by reflection because the values are
    /// string literals at the emission sites; a seventh reason added without a
    /// matching prime is caught by
    /// <see cref="Every_armed_rejection_reason_is_also_primed"/>, which reads the
    /// source rather than this list.
    /// </summary>
    private static readonly string[] RejectionReasons =
    [
        "argument_out_of_range_min",
        "argument_out_of_range_max",
        "already_in_progress",
        "shrink_unsupported",
        "resize_in_flight",
        "state_write_failed",
    ];

    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForRejectionReasons()
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ShardRootReshardRejected,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? reason = null;
                var onThisTree = false;

                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason && tag.Value is string arm)
                    {
                        reason = arm;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree)
                    {
                        onThisTree = string.Equals(tree, TreeId, StringComparison.Ordinal);
                    }
                }

                if (!onThisTree || reason is null)
                {
                    return;
                }

                lock (totals)
                {
                    totals[reason] = totals.TryGetValue(reason, out var running) ? running + value : value;
                }
            }));

        return (listener, totals);
    }

    /// <summary>
    /// A reshard that is accepted must still leave all six rejection reasons
    /// published at zero.
    /// <para>
    /// The scenario is the idempotent re-pin - a caller asking for the count the
    /// tree is already at - because it is the ordinary accepted case, it arms
    /// none of the six, and before this fix it therefore published nothing at
    /// all. It is also the sharpest available demonstration of the
    /// prime-above-every-early-return rule: it returns before the resize
    /// interlock and before the state write, so <c>resize_in_flight</c> and
    /// <c>state_write_failed</c> can only be zero here if the prime sits above
    /// both. Reverting the prime block reddens this, because all six arms become
    /// absent rather than zero.
    /// </para>
    /// <para>
    /// The write count is asserted unchanged so a future change that turns this
    /// scenario into a real reshard fails here rather than quietly re-characterising
    /// what the test covers.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task An_accepted_reshard_primes_every_rejection_reason_at_zero()
    {
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);
        var writesBefore = state.WriteCount;
        var (listener, totals) = ListenForRejectionReasons();

        await grain.ReshardAsync(2);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "the re-pin must remain the early-return path this fixture characterises; if it now writes "
                + "state, the two arms below the write are no longer being proved reachable by the prime");

            Assert.That(totals.Keys, Is.EquivalentTo(RejectionReasons),
                "every declared rejection reason must be published, so that an absent series means the build "
                + "does not carry the instrument and nothing else (issue #2918). A reason missing here is "
                + "unreadable in exactly the case a reader cares about - the refusal that never happens.");

            foreach (var reason in RejectionReasons)
            {
                Assert.That(totals.GetValueOrDefault(reason, -1), Is.Zero,
                    $"'{reason}' must be primed at zero, not incremented: this re-pin was accepted, so a "
                    + "non-zero total would mean the assertion is passing on a path it does not characterise.");
            }
        });
    }

    /// <summary>
    /// The positive control. A rejected reshard must be reported as a one on its
    /// own reason by this same listener, with the other five still at zero.
    /// <para>
    /// Without this, the test above is indistinguishable from a harness that
    /// observes nothing: a listener that enables no instrument, or a tag filter
    /// that never matches, produces exactly the zeros it asserts. "The value I
    /// expect is zero" and "I measured nothing" are the same assertion, so a
    /// priming test that does not also demonstrate the presence case is
    /// vacuous by construction.
    /// </para>
    /// <para>
    /// That the other five stay at zero is the second half of the control: it
    /// shows the harness attributes a measurement to the arm that produced it
    /// rather than to every arm it is watching.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public void A_rejected_reshard_is_reported_as_one_on_its_reason_by_this_same_harness()
    {
        const string Rejected = "shrink_unsupported";

        var (grain, _, _, _) = CreateGrain(physicalShardCount: 4);
        var (listener, totals) = ListenForRejectionReasons();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(2));

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(Rejected, Is.AnyOf(RejectionReasons),
                "the reason this control expects must be one the grain arms, or the control asserts against a "
                + "literal the source no longer uses");

            Assert.That(totals.GetValueOrDefault(Rejected, -1), Is.EqualTo(1),
                "this harness must observe a real rejection as a one. If it reports zero here, the zeros the "
                + "priming test asserts are 'the harness saw nothing' and that test is vacuous.");

            foreach (var reason in RejectionReasons.Where(r => !string.Equals(r, Rejected, StringComparison.Ordinal)))
            {
                Assert.That(totals.GetValueOrDefault(reason, -1), Is.Zero,
                    $"'{reason}' was not the refusal that occurred and must still read zero, which is what "
                    + "shows the harness discriminates between arms.");
            }
        });
    }

    /// <summary>
    /// Reads <c>TreeReshardGrain.cs</c> and requires that every <c>reason</c>
    /// value the grain arms with a one is also emitted with a zero somewhere in
    /// the same file.
    /// <para>
    /// This is the arm that survives a seventh reason being added. The two tests
    /// above enumerate the taxonomy from a list in this file, so a new rejection
    /// site would leave them green while its arm went unprimed - which is the
    /// original defect, reintroduced one value at a time. Checking the source
    /// makes the relation one the compiler cannot express but the gate can.
    /// </para>
    /// <para>
    /// The direction is deliberately one-way: every armed reason must be primed,
    /// not every primed reason must be armed. A prime for a reason whose site was
    /// removed is harmless (a permanent measured zero), whereas an arm without a
    /// prime is the defect.
    /// </para>
    /// </summary>
    [Test]
    public void Every_armed_rejection_reason_is_also_primed()
    {
        var source = File.ReadAllText(SourceFile("src/lattice/BPlusTree/Grains/TreeReshardGrain.cs"));

        var armed = System.Text.RegularExpressions.Regex.Matches(
                source,
                @"ShardRootReshardRejected\.Add\(1,[^;]*?""reason"",\s*""(?<r>[a-z_]+)""",
                System.Text.RegularExpressions.RegexOptions.Singleline)
            .Select(m => m.Groups["r"].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();

        var primed = System.Text.RegularExpressions.Regex.Matches(
                source,
                @"ShardRootReshardRejected\.Add\(0,[^;]*?""reason"",\s*""(?<r>[a-z_]+)""",
                System.Text.RegularExpressions.RegexOptions.Singleline)
            .Select(m => m.Groups["r"].Value)
            .Distinct(StringComparer.Ordinal)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(armed, Is.Not.Empty,
                "this gate must find the arming sites it audits. Zero matches means the regex no longer "
                + "describes the source, and the gate would pass while checking nothing.");

            Assert.That(armed, Is.EquivalentTo(RejectionReasons),
                "the reason list this fixture enumerates has drifted from the source. Update "
                + $"{nameof(RejectionReasons)} so the two tests above cover the real taxonomy.");

            Assert.That(armed.Where(r => !primed.Contains(r)), Is.Empty,
                "every reason the grain can arm must also be primed at zero (issue #2918), or that reason's "
                + "absence from a scrape means both 'it never happened' and 'the build cannot report it'.");
        });
    }

    /// <summary>
    /// Resolves a repository-relative path from the test assembly location, so
    /// the source-reading gate above works from any working directory.
    /// </summary>
    private static string SourceFile(string relative)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Orleans.Lattice.slnx")))
        {
            dir = dir.Parent;
        }

        Assert.That(dir, Is.Not.Null, "could not locate the repository root from the test assembly location");
        return Path.Combine(dir!.FullName, relative.Replace('/', Path.DirectorySeparatorChar));
    }
}
