using Orleans.Lattice.Internal.Cgroups;

namespace Orleans.Lattice.Tests.Internal.Cgroups;

/// <summary>
/// Covers the cgroup memory-limit reader. The parse cases moved here from
/// <c>LeafResidentWorkingSetTests</c> with issue #2828, when the reader left that
/// type for the shared cgroup folder; they now assert <see langword="null"/>
/// where they asserted zero, because the shared accessor spells unknown as
/// <see langword="null"/> for every reader.
/// </summary>
[TestFixture]
public sealed class ContainerMemoryLimitTests
{
    [Test]
    public void Parse_reads_a_real_limit()
    {
        Assert.That(ContainerMemoryLimit.Parse("12884901888\n"), Is.EqualTo(12884901888L));
    }

    [Test]
    public void Parse_treats_the_v2_unlimited_spelling_as_unknown()
    {
        Assert.That(ContainerMemoryLimit.Parse("max\n"), Is.Null);
    }

    [Test]
    public void Parse_treats_the_v1_saturation_sentinel_as_unknown()
    {
        // The arm that matters most. cgroup v1 spells unlimited as a page-aligned
        // saturation of the page counter, which is a perfectly well-formed
        // positive long. Believing it yields a budget of roughly two exabytes -
        // a bound that is present, plausible-looking, and unreachable. Unlike
        // "max" it cannot be caught by a parse failure, so it needs its own rule.
        // Both spellings are checked, including the unsigned one that overflows
        // long, which a signed cast would wrap to a negative.
        Assert.Multiple(() =>
        {
            Assert.That(
                ContainerMemoryLimit.Parse("9223372036854771712"),
                Is.Null,
                "the cgroup v1 page-counter saturation is unlimited, not a ceiling");
            Assert.That(
                ContainerMemoryLimit.Parse("18446744073709551615"),
                Is.Null,
                "an unsigned saturation that overflows long is unlimited, not a negative to be passed on");
        });
    }

    [Test]
    public void Parse_accepts_a_limit_just_below_the_sentinel_floor()
    {
        const long justBelow = ContainerMemoryLimit.UnlimitedSentinelFloor - 1;

        Assert.That(
            ContainerMemoryLimit.Parse(justBelow.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            Is.EqualTo(justBelow));
    }

    [Test]
    public void Parse_treats_unreadable_content_as_unknown()
    {
        Assert.Multiple(() =>
        {
            foreach (var body in new[] { null, string.Empty, "   ", "not-a-number", "-1", "0" })
            {
                Assert.That(
                    ContainerMemoryLimit.Parse(body),
                    Is.Null,
                    $"'{body ?? "<null>"}' is not a ceiling and must degrade to unknown");
            }
        });
    }

    [Test]
    public void Read_never_reports_a_non_positive_limit()
    {
        // Off Linux the accessor short-circuits to unknown. On Linux the answer
        // depends on the host, but a known answer must be a real ceiling.
        var limit = ContainerMemoryLimit.Read();

        if (!OperatingSystem.IsLinux())
        {
            Assert.That(limit, Is.Null);
            return;
        }

        Assert.That(limit is null || limit > 0, Is.True, $"read {limit}");
    }
}
