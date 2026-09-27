namespace Orleans.Lattice.Internal.Cgroups;

/// <summary>
/// Reads the container's enforced memory limit from the cgroup filesystem, so a
/// budget sized from memory can honour the limit the kernel will actually
/// enforce rather than the host's physical memory.
/// </summary>
/// <remarks>
/// <para>
/// Moved here from <c>LeafResidentWorkingSet</c> by issue #2828, where it was the
/// third of three independent cgroup readers and the one that sat outside the
/// guard that kept the other two in step. It now holds only the memory-specific
/// parsing; the probe, the defensive read, the off-Linux short-circuit, and the
/// spelling of "unknown" belong to <see cref="CgroupFileSystem"/>, which
/// <see cref="ContainerCpuGrant"/> goes through as well.
/// </para>
/// <para>
/// Every failure degrades to unknown, which the consumers map to their
/// behaviour before cgroup detection existed. Detection failing is therefore
/// never worse than not detecting, which is what licenses the deliberately
/// narrow probe: the two canonical mount paths and nothing else. A process in
/// an exotic cgroup layout gets the budget it had before, not a wrong one.
/// </para>
/// </remarks>
internal static class ContainerMemoryLimit
{
    /// <summary>The cgroup v2 unified memory limit file.</summary>
    public const string CgroupV2MemoryMaxPath = "/sys/fs/cgroup/memory.max";

    /// <summary>The cgroup v1 memory limit file.</summary>
    public const string CgroupV1LimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

    /// <summary>
    /// At or above this, a cgroup memory limit is read as "unlimited" rather
    /// than as a ceiling. cgroup v1 spells unlimited as a page-aligned
    /// saturation of the page counter near <see cref="long.MaxValue"/>, which is
    /// a well-formed positive number and would otherwise be believed.
    /// <para>
    /// 4 EiB is not a boundary any real deployment sits near, so this does not
    /// trade a false positive for a false negative: no container is granted
    /// exabytes, and a limit that large is unlimited in every sense that matters
    /// to a budget denominated in bytes.
    /// </para>
    /// </summary>
    public const long UnlimitedSentinelFloor = 1L << 62;

    private static readonly string[] Paths = [CgroupV2MemoryMaxPath, CgroupV1LimitPath];

    /// <summary>
    /// Reads the enforced container memory limit in bytes.
    /// </summary>
    /// <returns>
    /// The limit, or <see langword="null"/> when there is none, the platform has
    /// no cgroups, or no candidate file could be read or parsed.
    /// </returns>
    public static long? Read() => CgroupFileSystem.ReadFirstKnown(Paths, Parse);

    /// <summary>
    /// Parses a cgroup memory limit file body, returning <see langword="null"/>
    /// for every form that means "no limit" and for every form that cannot be
    /// read as one.
    /// </summary>
    /// <param name="contents">The raw file content.</param>
    /// <returns>A positive limit in bytes, or <see langword="null"/>.</returns>
    /// <remarks>
    /// Three distinct spellings of unlimited have to be recognised, and missing
    /// any one of them yields a budget derived from a nonsense ceiling rather
    /// than a safe fallback:
    /// <list type="bullet">
    /// <item>cgroup v2 writes the literal string <c>max</c>;</item>
    /// <item>cgroup v1 writes a page-aligned saturation of the counter, which is
    /// a positive <see cref="long"/> near <see cref="long.MaxValue"/> and so
    /// parses perfectly well as a number - this is the one that does damage
    /// quietly, because it divides into a budget of exabytes that no bound can
    /// ever reach;</item>
    /// <item>some kernels write that same saturation as an <b>unsigned</b>
    /// 64-bit value that overflows <see cref="long"/> entirely.</item>
    /// </list>
    /// <para>
    /// Comparing the sentinel in <see cref="ulong"/> space lets one clause cover
    /// both saturation spellings, and the shape is the result of a perturbation
    /// arm rather than of taste. An earlier revision (in
    /// <c>LeafResidentWorkingSet</c>) carried explicit <c>max</c>/empty and
    /// <c>value &gt; (ulong)long.MaxValue</c> overflow branches, and reverting
    /// each in isolation reddened nothing, because the unsigned parse already
    /// rejects <c>max</c> and empty text and the <see cref="ulong"/> comparison
    /// already catches the overflow. Dead clauses read as careful handling, which
    /// is worse than no handling because it invites trust.
    /// </para>
    /// <para>
    /// The zero clause is the exception, and it is live here where it was dead
    /// there. That reader spelled unknown as zero, so a zero limit already meant
    /// unknown; this one spells unknown as <see langword="null"/>, so without the
    /// clause a zero would be reported as a known ceiling of zero and would also
    /// stop <see cref="CgroupFileSystem.ReadFirstKnown{T}"/> from trying the next
    /// candidate path.
    /// </para>
    /// </remarks>
    public static long? Parse(string? contents)
    {
        var text = contents?.Trim();

        if (!ulong.TryParse(text, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var value))
        {
            return null;
        }

        return value == 0 || value >= (ulong)UnlimitedSentinelFloor ? null : (long)value;
    }
}
