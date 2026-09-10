namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// Reads the container's enforced CPU grant from the cgroup filesystem.
/// </summary>
/// <remarks>
/// <para>
/// This exists because <see cref="System.Environment.ProcessorCount"/> is not a
/// reliable statement of how much CPU the process may actually use. It is
/// quota-derived only when nothing overrides it, and <c>DOTNET_PROCESSOR_COUNT</c>
/// overrides it. Measured on .NET 10 under Docker, all four combinations behave
/// as follows:
/// </para>
/// <list type="table">
///   <listheader>
///     <term>Container limit</term>
///     <description>cpu.max / ProcessorCount</description>
///   </listheader>
///   <item>
///     <term><c>--cpus=4</c></term>
///     <description><c>"400000 100000"</c> / 4. Quota-derived, correct.</description>
///   </item>
///   <item>
///     <term><c>--cpus=4</c> plus <c>DOTNET_PROCESSOR_COUNT=16</c></term>
///     <description><c>"400000 100000"</c> / <b>16</b>. The override wins and the
///     quota is ignored.</description>
///   </item>
///   <item>
///     <term><c>--cpus=4.5</c></term>
///     <description><c>"450000 100000"</c> / 5. The ceiling is taken, so a
///     fractional grant is rounded up and is mildly oversubscribed by design.</description>
///   </item>
///   <item>
///     <term>no limit</term>
///     <description><c>"max 100000"</c> / 16 (the host core count), which is
///     correct because nothing is being enforced.</description>
///   </item>
/// </list>
/// <para>
/// The second row is the reason this type exists. The sample compose project
/// sets <c>DOTNET_PROCESSOR_COUNT</c> on the repository-context service (it sizes
/// an unrelated write-ahead-log replay gate), so an operator copying that
/// service's environment block onto this one is an entirely ordinary thing to do,
/// and it would silently restore the thread oversubscription this server now
/// avoids. Reading the quota directly is immune to that, because the quota is the
/// figure the kernel enforces rather than a figure something else may have
/// declared.
/// </para>
/// </remarks>
internal static class ContainerCpuGrant
{
    /// <summary>The cgroup v2 unified CPU limit file.</summary>
    public const string CgroupV2CpuMaxPath = "/sys/fs/cgroup/cpu.max";

    /// <summary>The cgroup v1 CPU quota file.</summary>
    public const string CgroupV1QuotaPath = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";

    /// <summary>The cgroup v1 CPU period file.</summary>
    public const string CgroupV1PeriodPath = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    /// <summary>
    /// Reads the enforced CPU grant, preferring cgroup v2 and falling back to
    /// cgroup v1.
    /// </summary>
    /// <returns>The whole number of CPUs the container may use, or
    /// <see langword="null"/> when no quota is enforced or none could be read.
    /// A null result is not an error: it is the correct answer on an
    /// unconstrained host and on a non-Linux machine.</returns>
    public static int? Read()
    {
        var v2 = TryReadAllText(CgroupV2CpuMaxPath);
        if (v2 is not null)
        {
            return ParseCpuMax(v2);
        }

        return ParseCpuQuota(
            TryReadAllText(CgroupV1QuotaPath), TryReadAllText(CgroupV1PeriodPath));
    }

    /// <summary>
    /// Parses a cgroup v2 <c>cpu.max</c> payload, which is a quota and a period
    /// separated by whitespace, where the quota may be the literal
    /// <c>max</c>.
    /// </summary>
    /// <param name="content">The raw file content.</param>
    /// <returns>The whole number of CPUs, or <see langword="null"/> when
    /// unlimited or unparseable.</returns>
    public static int? ParseCpuMax(string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return null;
        }

        var parts = content.Trim().Split(
            (char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            return null;
        }

        return ParseCpuQuota(parts[0], parts[1]);
    }

    /// <summary>
    /// Converts a quota and period pair into a whole number of CPUs, taking the
    /// ceiling so a fractional grant is never rounded down to a smaller pool
    /// than the container was given.
    /// </summary>
    /// <param name="quota">The quota in microseconds, or <c>max</c> or a
    /// negative value for unlimited.</param>
    /// <param name="period">The period in microseconds.</param>
    /// <returns>The whole number of CPUs, or <see langword="null"/> when
    /// unlimited or unparseable.</returns>
    public static int? ParseCpuQuota(string? quota, string? period)
    {
        if (quota is null || period is null)
        {
            return null;
        }

        var trimmedQuota = quota.Trim();
        if (string.Equals(trimmedQuota, "max", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (!long.TryParse(trimmedQuota, out var quotaValue) || quotaValue <= 0)
        {
            return null;
        }

        if (!long.TryParse(period.Trim(), out var periodValue) || periodValue <= 0)
        {
            return null;
        }

        var cpus = (int)Math.Ceiling((double)quotaValue / periodValue);
        return Math.Max(1, cpus);
    }

    private static string? TryReadAllText(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path) : null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }
}
