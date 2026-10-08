using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Tests for the <c>changed</c> flag <see cref="Orleans.Lattice.Views.HistoryRetentionShaper.Shape(Orleans.Lattice.HistoryRow, Orleans.Lattice.Views.HistoryRetentionPolicy, long)"/>
/// reports, which the view maintainer uses to keep the bytes it already holds
/// instead of re-serialising a row that shaping did not alter.
/// <para>
/// The flag is only safe if it is exactly a claim about the shaped row: whenever
/// it is <see langword="false"/> the shaped row must equal the input row, and
/// whenever the shaped row differs it must be <see langword="true"/>. A flag that
/// under-reports would persist an unshaped row - leaking value bytes an operator
/// configured away - so these tests assert the biconditional over every kind and
/// mode rather than spot-checking the cases the trim was written for.
/// </para>
/// </summary>
[TestFixture]
public sealed class HistoryRetentionShaperChangeReportTests
{
    private static readonly long Now = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static HistoryRow Row(HistoryRowKind kind, HistoryRetentionMode shape, byte[]? value) => new()
    {
        Timestamp = new HybridLogicalClock { WallClockTicks = Now - 1_000, Counter = 0 },
        Kind = kind,
        SourceKey = "k",
        Value = value,
        ValueHash = value is null ? 0 : 123,
        ValueLength = value?.Length ?? 0,
        Delta = kind == HistoryRowKind.CrdtDelta ? [9, 9, 9] : null,
        RetentionShape = shape,
    };

    private static IEnumerable<TestCaseData> EveryCombination()
    {
        foreach (var kind in Enum.GetValues<HistoryRowKind>())
        {
            foreach (var stamped in Enum.GetValues<HistoryRetentionMode>())
            {
                foreach (var mode in Enum.GetValues<HistoryRetentionMode>())
                {
                    foreach (var hasValue in new[] { true, false })
                    {
                        yield return new TestCaseData(kind, stamped, mode, hasValue)
                            .SetName($"Shape_reports_change_iff_the_row_changed({kind},{stamped}->{mode},value={hasValue})");
                    }
                }
            }
        }
    }

    /// <summary>
    /// The load-bearing property: <c>changed</c> is true exactly when the shaped
    /// row differs from the input. Anything weaker makes the elided re-encode a
    /// correctness bug rather than a trim.
    /// </summary>
    [TestCaseSource(nameof(EveryCombination))]
    public void Shape_reports_change_iff_the_row_changed(
        HistoryRowKind kind,
        HistoryRetentionMode stamped,
        HistoryRetentionMode mode,
        bool hasValue)
    {
        var row = Row(kind, stamped, hasValue ? [1, 2, 3] : null);
        var policy = new HistoryRetentionPolicy(mode, TimeSpan.Zero, TimeSpan.FromHours(1));

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now, out var changed);

        Assert.That(changed, Is.EqualTo(!shaped.Equals(row)));
    }

    /// <summary>
    /// The case the trim exists for: the projection never stamps
    /// <c>RetentionShape</c>, so a row arrives carrying the enum default, which is
    /// also the default policy. Under it, every kind that carries no strippable
    /// LWW value is already in shape.
    /// </summary>
    [TestCase(HistoryRowKind.Delete)]
    [TestCase(HistoryRowKind.CrdtDelta)]
    [TestCase(HistoryRowKind.RangeTombstone)]
    public void Shape_reports_no_change_for_an_unstamped_non_set_row_under_the_default_policy(HistoryRowKind kind)
    {
        var row = Row(kind, default, value: null);
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.Zero, TimeSpan.Zero);

        HistoryRetentionShaper.Shape(row, policy, Now, out var changed);

        Assert.That(changed, Is.False);
    }

    /// <summary>An LWW set row under metadata-only retention genuinely loses its bytes.</summary>
    [Test]
    public void Shape_reports_change_for_a_set_row_whose_value_is_stripped()
    {
        var row = Row(HistoryRowKind.Set, default, [1, 2, 3]);
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.Zero, TimeSpan.Zero);

        HistoryRetentionShaper.Shape(row, policy, Now, out var changed);

        Assert.That(changed, Is.True);
    }

    /// <summary>
    /// An age bound is stamped on the view entry, not inside the row, so it must
    /// never on its own report the row as changed.
    /// </summary>
    [Test]
    public void Shape_reports_no_change_when_only_the_expiry_is_stamped()
    {
        var row = Row(HistoryRowKind.Delete, default, value: null);
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.FromDays(7), TimeSpan.Zero);

        var (_, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, Now, out var changed);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False);
            Assert.That(expiresAtTicks, Is.EqualTo(Now + TimeSpan.FromDays(7).Ticks));
        });
    }

    /// <summary>
    /// The overload without the flag is the oracle the flagged one is specified
    /// against, so the two must never disagree on the row or the expiry.
    /// </summary>
    [TestCaseSource(nameof(EveryCombination))]
    public void Shape_overloads_agree_on_the_row_and_the_expiry(
        HistoryRowKind kind,
        HistoryRetentionMode stamped,
        HistoryRetentionMode mode,
        bool hasValue)
    {
        var row = Row(kind, stamped, hasValue ? [1, 2, 3] : null);
        var policy = new HistoryRetentionPolicy(mode, TimeSpan.FromDays(1), TimeSpan.FromHours(1));

        var plain = HistoryRetentionShaper.Shape(row, policy, Now);
        var flagged = HistoryRetentionShaper.Shape(row, policy, Now, out _);

        Assert.Multiple(() =>
        {
            Assert.That(flagged.Row, Is.EqualTo(plain.Row));
            Assert.That(flagged.ExpiresAtTicks, Is.EqualTo(plain.ExpiresAtTicks));
        });
    }
}
