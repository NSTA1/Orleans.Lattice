using Orleans.Lattice.Internal.Cgroups;

namespace Orleans.Lattice.Tests.Internal.Cgroups;

/// <summary>
/// Covers the shared cgroup CPU-grant reader that decouples pool sizing from
/// <see cref="System.Environment.ProcessorCount"/>. Moved here with issue #2613
/// when the reader was promoted out of the ONNX embedding app, and moved again
/// with issue #2816 when it was promoted from the repository-context add-on into
/// the core library so the WAL replay concurrency gate could consult it too.
/// </summary>
/// <remarks>
/// The original examples were measured on .NET 10 under Docker. Boundary and
/// malformed-input cases also pin the parser's behavior beyond those payloads.
/// </remarks>
[TestFixture]
public sealed class ContainerCpuGrantTests
{
    [Test]
    public void ParseCpuMax_reads_a_whole_cpu_grant()
    {
        Assert.That(ContainerCpuGrant.ParseCpuMax("400000 100000"), Is.EqualTo(4));
    }

    [Test]
    public void ParseCpuMax_returns_null_for_an_unlimited_grant()
    {
        Assert.That(ContainerCpuGrant.ParseCpuMax("max 100000"), Is.Null,
            "an unlimited cgroup must not be mistaken for a one-CPU grant.");
    }

    [Test]
    public void ParseCpuMax_takes_the_ceiling_of_a_fractional_grant()
    {
        Assert.That(ContainerCpuGrant.ParseCpuMax("450000 100000"), Is.EqualTo(5),
            "rounding up matches what .NET itself reports for --cpus=4.5, so the "
            + "derived pool never disagrees with the runtime in the safe direction.");
    }

    [Test]
    public void ParseCpuMax_tolerates_a_trailing_newline()
    {
        Assert.That(ContainerCpuGrant.ParseCpuMax("400000 100000\n"), Is.EqualTo(4));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("400000")]
    [TestCase("nonsense payload")]
    [TestCase("max")]
    [TestCase("400000 0")]
    [TestCase("400000 -1")]
    [TestCase("0 100000")]
    [TestCase("-1 100000")]
    public void ParseCpuMax_returns_null_for_unusable_content(string? content)
    {
        Assert.That(ContainerCpuGrant.ParseCpuMax(content), Is.Null);
    }

    [Test]
    public void ParseCpuQuota_reads_a_cgroup_v1_pair()
    {
        Assert.That(ContainerCpuGrant.ParseCpuQuota("400000", "100000"), Is.EqualTo(4));
    }

    [Test]
    public void ParseCpuQuota_returns_null_for_the_v1_unlimited_sentinel()
    {
        Assert.That(ContainerCpuGrant.ParseCpuQuota("-1", "100000"), Is.Null,
            "cgroup v1 spells unlimited as a negative quota rather than as max.");
    }

    [TestCase(null, "100000")]
    [TestCase("400000", null)]
    [TestCase("400000", "0")]
    [TestCase("abc", "100000")]
    [TestCase("400000", "abc")]
    [TestCase("", "100000")]
    [TestCase(" ", "100000")]
    [TestCase("0", "100000")]
    [TestCase("-2", "100000")]
    [TestCase("400000", "")]
    [TestCase("400000", " ")]
    [TestCase("400000", "-1")]
    [TestCase("400000", "max")]
    [TestCase("9223372036854775808", "1")]
    [TestCase("1", "9223372036854775808")]
    public void ParseCpuQuota_returns_null_for_unusable_pairs(string? quota, string? period)
    {
        Assert.That(ContainerCpuGrant.ParseCpuQuota(quota, period), Is.Null);
    }

    [TestCase("max")]
    [TestCase("MAX")]
    [TestCase(" Max \n")]
    public void ParseCpuQuota_v2_unlimited_sentinel_is_not_a_numeric_quota(string quota)
    {
        Assert.Multiple(() =>
        {
            Assert.That(long.TryParse(quota.Trim(), out _), Is.False,
                "the v2 unlimited sentinel is rejected by numeric parsing itself.");
            Assert.That(ContainerCpuGrant.ParseCpuQuota(quota, "100000"), Is.Null);
            Assert.That(ContainerCpuGrant.ParseCpuMax($"{quota} 100000"), Is.Null);
        });
    }

    [TestCase("1000", "100000", 1)]
    [TestCase("1", "9223372036854775807", 1)]
    [TestCase("1", "1", 1)]
    [TestCase("100001", "100000", 2)]
    [TestCase("450000", "100000", 5)]
    [TestCase(" 400000\n", "\t100000 ", 4)]
    [TestCase("2147483646", "1", 2147483646)]
    [TestCase("2147483647", "1", int.MaxValue)]
    [TestCase("2147483648", "1", int.MaxValue)]
    [TestCase("9000000000000000000", "1", int.MaxValue)]
    [TestCase("9223372036854775807", "1", int.MaxValue)]
    public void ParseCpuQuota_positive_ratio_rounds_up_and_saturates_at_int_max(
        string quota, string period, int expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ContainerCpuGrant.ParseCpuQuota(quota, period), Is.EqualTo(expected));
            Assert.That(ContainerCpuGrant.ParseCpuMax($"{quota} {period}"), Is.EqualTo(expected));
        });
    }

    [Test]
    public void Read_returns_null_or_a_positive_grant()
    {
        var grant = ContainerCpuGrant.Read();

        Assert.That(grant is null || grant > 0, Is.True,
            "Read must be safe to call off Linux, where no cgroup files exist and "
            + "the correct answer is that no quota is enforced.");
    }

    [Test]
    public void Cgroup_paths_are_absolute()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ContainerCpuGrant.CgroupV2CpuMaxPath, Does.StartWith("/sys/fs/cgroup"));
            Assert.That(ContainerCpuGrant.CgroupV1QuotaPath, Does.StartWith("/sys/fs/cgroup"));
            Assert.That(ContainerCpuGrant.CgroupV1PeriodPath, Does.StartWith("/sys/fs/cgroup"));
        });
    }
}
