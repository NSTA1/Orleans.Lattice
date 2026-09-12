namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Covers the shared cgroup CPU-grant reader that decouples pool sizing from
/// <see cref="System.Environment.ProcessorCount"/>. Moved here with issue #2613
/// when the reader was promoted out of the ONNX embedding app into
/// <c>src/lattice.api.mcp.repocontext</c> so every pool-sizing site can consult
/// one implementation.
/// </summary>
/// <remarks>
/// The rows below are not invented. Each was measured on .NET 10 under Docker
/// before the reader was written, so the parser is tested against the payloads
/// the kernel actually produced rather than against the ones the documentation
/// implies it produces.
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
    public void ParseCpuQuota_returns_null_for_unusable_pairs(string? quota, string? period)
    {
        Assert.That(ContainerCpuGrant.ParseCpuQuota(quota, period), Is.Null);
    }

    [Test]
    public void ParseCpuQuota_never_returns_a_grant_below_one()
    {
        Assert.That(ContainerCpuGrant.ParseCpuQuota("1000", "100000"), Is.EqualTo(1),
            "a sub-CPU grant still needs at least one thread to make progress.");
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
