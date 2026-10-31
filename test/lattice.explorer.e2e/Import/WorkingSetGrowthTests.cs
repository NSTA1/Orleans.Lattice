namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The arithmetic of the bounded-memory proof (#5475), on readings recorded by real CI runs
/// of <see cref="ImportMemoryTests"/>: the growth an import is charged with must be memory the
/// file's import added, not memory the server garbage collector happened to return between
/// the warm-up and the baseline.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class WorkingSetGrowthTests
{
    private const long Mb = 1024L * 1024;

    /// <summary>
    /// Explorer Journeys run 41236623388, first attempt, west-silo-0 during the 200 MB upsert:
    /// the warm-up took the silo to 498.6 MB, the collector returned about 100 MB before the
    /// baseline was read (399.9 MB), and the 200 MB import peaked at 516.4 MB. The import
    /// added 17.8 MB to the working set the same path had already reached; it was charged
    /// with 116.5 MB.
    /// </summary>
    [Test]
    public void Growth_is_measured_from_the_working_set_the_warm_up_reached_not_from_a_collected_baseline()
    {
        var reading = new WorkingSetGrowth("west-silo-0", Baseline: 3999 * Mb / 10, Peak: 5164 * Mb / 10, Samples: 94)
        {
            WarmUpPeak = 4986 * Mb / 10,
        };

        Assert.That(reading.Growth, Is.EqualTo(reading.Peak - reading.WarmUpPeak));
    }

    /// <summary>
    /// When the baseline is above the warm-up's peak, the baseline is the reference: the
    /// import is charged with what it added above the larger of the two.
    /// </summary>
    [Test]
    public void Growth_is_measured_from_the_baseline_when_it_is_above_the_warm_up_peak()
    {
        var reading = new WorkingSetGrowth("west-silo-0", Baseline: 5093 * Mb / 10, Peak: 5470 * Mb / 10, Samples: 113)
        {
            WarmUpPeak = 5038 * Mb / 10,
        };

        Assert.That(reading.Growth, Is.EqualTo(reading.Peak - reading.Baseline));
    }
}
