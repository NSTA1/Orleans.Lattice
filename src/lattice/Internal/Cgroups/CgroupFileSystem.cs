namespace Orleans.Lattice.Internal.Cgroups;

/// <summary>
/// The single accessor every cgroup reader in the repository goes through. It
/// owns the three steps that are the same whatever is being read - the ordered
/// path probe, the defensive read, and the degrade-to-unknown policy - so the
/// per-concern readers (<see cref="ContainerCpuGrant"/>,
/// <see cref="ContainerMemoryLimit"/>) contain nothing but their own parser.
/// </summary>
/// <remarks>
/// <para>
/// Before issue #2828 the repository held three independent readers of the
/// cgroup filesystem, and the five-step mechanism was implemented once per
/// reader. The answers they reached diverged by author rather than by design:
/// one short-circuited off Linux and one did not, one spelled "unknown" as
/// <see langword="null"/> and one as zero, and the "never fault the caller on an
/// unreadable cgroup file" policy was asserted twice in two files with nothing
/// tying them together. This type is where those decisions are now made, once:
/// </para>
/// <list type="bullet">
///   <item><b>Unknown is <see langword="null"/>.</b> Every reader returns a
///   nullable figure, so "no limit" and "could not read" can never be mistaken
///   for a limit of zero.</item>
///   <item><b>Off Linux the answer is unknown, without touching the disk.</b>
///   cgroups are a Linux kernel facility; on any other platform the canonical
///   paths resolve against the current drive root, where a stray file would be
///   believed. <see cref="IsSupported"/> is consulted by every public entry
///   point.</item>
///   <item><b>Nothing throws.</b> A missing, unreadable, or permission-denied
///   file degrades to unknown, and a caller that sizes a pool or a budget from
///   the result falls back to its own default.</item>
///   <item><b>First known value wins.</b> Candidate paths are probed in the
///   order given (cgroup v2 first by convention), and a path that exists but
///   parses to unknown - v2's <c>max</c>, say - does not stop the probe.</item>
/// </list>
/// <para>
/// This folder (<c>src/lattice/Internal/Cgroups/</c>) is compiled into two
/// assemblies: the core library, and the standalone ONNX embedding companion
/// under <c>apps/embedding-onnx</c>, which links these sources and receives them
/// in its container image through a BuildKit named context (issue #2817). Every
/// file here must therefore depend on the base class library alone. The
/// <c>CgroupSourcesCompileStandaloneTests</c> fixture enforces that.
/// </para>
/// </remarks>
internal static class CgroupFileSystem
{
    /// <summary>
    /// Whether the running platform has a cgroup filesystem to read at all.
    /// When <see langword="false"/>, every read reports unknown without probing.
    /// </summary>
    public static bool IsSupported => OperatingSystem.IsLinux();

    /// <summary>
    /// Reads a cgroup file, degrading every failure to <see langword="null"/>.
    /// </summary>
    /// <param name="path">The absolute path of the cgroup file.</param>
    /// <returns>The file's content, or <see langword="null"/> off Linux, when
    /// the file is absent, or when it cannot be read.</returns>
    public static string? TryReadAllText(string path)
        => IsSupported ? TryReadFile(path) : null;

    /// <summary>
    /// Probes <paramref name="paths"/> in order and returns the first value
    /// <paramref name="parse"/> recognises as known.
    /// </summary>
    /// <typeparam name="T">The parsed figure.</typeparam>
    /// <param name="paths">Candidate cgroup files, most preferred first.</param>
    /// <param name="parse">Maps a file's content to a known figure, or to
    /// <see langword="null"/> for unlimited or unparseable content.</param>
    /// <returns>The first known figure, or <see langword="null"/> when none is
    /// known, off Linux, or when no candidate could be read.</returns>
    public static T? ReadFirstKnown<T>(ReadOnlySpan<string> paths, Func<string, T?> parse)
        where T : struct
        => IsSupported ? ProbeFirstKnown(paths, parse) : null;

    /// <summary>
    /// The platform-independent core of <see cref="ReadFirstKnown{T}"/>, split
    /// out so the probe order and the skip-unknown rule are testable on any
    /// operating system against ordinary temporary files.
    /// </summary>
    internal static T? ProbeFirstKnown<T>(ReadOnlySpan<string> paths, Func<string, T?> parse)
        where T : struct
    {
        ArgumentNullException.ThrowIfNull(parse);

        foreach (var path in paths)
        {
            var content = TryReadFile(path);
            if (content is null)
            {
                continue;
            }

            var value = parse(content);
            if (value is not null)
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// The platform-independent core of <see cref="TryReadAllText"/>: the one
    /// place the "never fault the caller on an unreadable cgroup file" policy
    /// lives.
    /// </summary>
    internal static string? TryReadFile(string path)
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
