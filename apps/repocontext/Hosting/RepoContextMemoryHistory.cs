using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The durable record of what this deployment's memory requirement was measured to
/// be, carried across a restart on the container's data mount so the next process
/// can relate the ceiling it is granted to the commitment the last one actually
/// reached.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a file and not a metric.</b> Same reason as
/// <see cref="RepoContextDrainHistory"/>, whose shape this deliberately mirrors: the
/// measurement that matters is taken by a process that does not survive to be
/// scraped about it. An undersized grant here does not kill the container - it
/// produces a wave of <see cref="OutOfMemoryException"/> behind a STORAGE fault and
/// a crash-loop - so the only consumer that reliably exists afterwards is the next
/// process, and the only channel that reaches it is the data mount both share.
/// </para>
/// <para>
/// <b>IT FAILS OPEN, AND THAT IS A DELIBERATE INVERSION OF THE PRECEDENT IT
/// OTHERWISE COPIES.</b> <see cref="RepoContextDrainHistory"/> documents itself as
/// failing soft "in both directions" because it is pure diagnostics and "a corrupt
/// diagnostic must never stop a container starting". This store is not pure
/// diagnostics: it feeds <see cref="RepoContextMemoryAdmission"/>, which is the
/// first thing in this container that can refuse to start at all. The precedent's
/// symmetry therefore must <i>not</i> be carried over unexamined, and the direction
/// has to be stated rather than inherited:
/// </para>
/// <list type="bullet">
/// <item><description>
/// A file that is missing, unreadable, unparseable, truncated, or written in a
/// format version this build does not recognise reads as <see langword="null"/> -
/// which <see cref="RepoContextMemoryAdmission"/> treats as <b>admit</b>.
/// </description></item>
/// <item><description>
/// Only a positively parsed record carrying a positively matching claim can refuse.
/// </description></item>
/// </list>
/// <para>
/// The reflex on a safety check is to fail closed, and here that reflex is wrong.
/// Failing closed would mean a parse bug, a half-written file from a process killed
/// mid-write, or simply a newer build's format bricks a container that would have
/// run perfectly well - converting a diagnostic defect into an outage. The asymmetry
/// is the whole design: a false admit costs exactly the status quo this change is
/// improving on, while a false refusal costs an outage with no shell to debug it in.
/// </para>
/// <para>
/// <b>The format is line-based <c>key=value</c> text, not JSON</b>, for the reasons
/// the drain store gives: the cheapest possible parse at both ends, no serializer
/// configuration to get wrong, and a file that survives being looked at by eye
/// through a mounted volume - which matters more here than there, because the image
/// is distroless and there is no shell to inspect it with. An unrecognised key is
/// ignored and a missing optional key reads as absent, so a field added later is
/// backward-compatible with no version negotiation.
/// </para>
/// </remarks>
public static class RepoContextMemoryHistory
{
    /// <summary>The file name written under the container's data directory.</summary>
    public const string FileName = "heap-history.txt";

    /// <summary>
    /// The format version written into the file. A file declaring a version this
    /// build does not recognise is treated as absent - and therefore as admitting -
    /// rather than guessed at.
    /// </summary>
    public const int FormatVersion = 1;

    private const string VersionKey = "version";
    private const string ObservedAtKey = "observedAtUtc";
    private const string OutcomeKey = "outcome";
    private const string GrantedLimitKey = "grantedLimitBytes";
    private const string PeakCommittedKey = "peakCommittedBytes";
    private const string ExhaustionEventsKey = "exhaustionEvents";
    private const string ExhaustedAtKey = "exhaustedAtLimitBytes";
    private const string OverriddenAtKey = "overriddenAtLimitBytes";

    /// <summary>
    /// Resolves the history file's path inside <paramref name="directory"/>.
    /// </summary>
    /// <param name="directory">The container's durable data directory.</param>
    /// <returns>The full path to the history file.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="directory"/> is null.</exception>
    public static string PathIn(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        return Path.Combine(directory, FileName);
    }

    /// <summary>
    /// Reads the last recorded run, or <see langword="null"/> when there is no
    /// readable record.
    /// </summary>
    /// <remarks>
    /// Every failure path returns <see langword="null"/> rather than throwing. See
    /// the fail-open remarks on this type: a read that cannot produce a record must
    /// admit, never refuse.
    /// </remarks>
    /// <param name="path">The history file's path.</param>
    /// <returns>The recorded observation, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is null.</exception>
    public static RepoContextMemoryObservation? Read(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        string[] lines;
        try
        {
            if (!File.Exists(path))
            {
                return null;
            }

            lines = File.ReadAllLines(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
        {
            return null;
        }

        return Parse(lines);
    }

    /// <summary>
    /// Parses the history file's lines. Exposed so the format can be tested without
    /// touching the file system.
    /// </summary>
    /// <param name="lines">The file's lines.</param>
    /// <returns>
    /// The recorded observation, or <see langword="null"/> when the content is not a
    /// readable record. Null is the admitting answer.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="lines"/> is null.</exception>
    public static RepoContextMemoryObservation? Parse(IEnumerable<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in lines)
        {
            if (line is null)
            {
                continue;
            }

            var separator = line.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                continue;
            }

            fields[line[..separator].Trim()] = line[(separator + 1)..].Trim();
        }

        if (!fields.TryGetValue(VersionKey, out var rawVersion)
            || !int.TryParse(rawVersion, NumberStyles.Integer, CultureInfo.InvariantCulture, out var version)
            || version != FormatVersion)
        {
            return null;
        }

        if (!fields.TryGetValue(OutcomeKey, out var rawOutcome)
            || !Enum.TryParse<RepoContextMemoryOutcome>(rawOutcome, ignoreCase: true, out var outcome)
            || !Enum.IsDefined(outcome))
        {
            return null;
        }

        if (!fields.TryGetValue(ObservedAtKey, out var rawObservedAt)
            || !DateTimeOffset.TryParse(
                rawObservedAt,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var observedAt))
        {
            return null;
        }

        if (!TryReadRequiredBytes(fields, GrantedLimitKey, out var granted)
            || !TryReadRequiredBytes(fields, PeakCommittedKey, out var peak)
            || !TryReadRequiredBytes(fields, ExhaustionEventsKey, out var events))
        {
            return null;
        }

        if (!TryReadOptionalBytes(fields, ExhaustedAtKey, out var exhaustedAt)
            || !TryReadOptionalBytes(fields, OverriddenAtKey, out var overriddenAt))
        {
            return null;
        }

        return new RepoContextMemoryObservation(
            observedAt,
            outcome,
            granted,
            peak,
            events,
            exhaustedAt,
            overriddenAt);
    }

    /// <summary>
    /// Renders an observation in the file's format. Exposed so the round trip is
    /// testable without a file system, and so a caller can log exactly what was
    /// written.
    /// </summary>
    /// <param name="observation">The observation to render.</param>
    /// <returns>The file content.</returns>
    public static string Render(RepoContextMemoryObservation observation)
    {
        var lines = new List<string>(8)
        {
            $"{VersionKey}={FormatVersion.ToString(CultureInfo.InvariantCulture)}",
            $"{ObservedAtKey}={observation.ObservedAtUtc.ToUniversalTime():O}",
            $"{OutcomeKey}={observation.Outcome}",
            $"{GrantedLimitKey}={observation.GrantedLimitBytes.ToString(CultureInfo.InvariantCulture)}",
            $"{PeakCommittedKey}={observation.PeakCommittedBytes.ToString(CultureInfo.InvariantCulture)}",
            $"{ExhaustionEventsKey}={observation.ExhaustionEvents.ToString(CultureInfo.InvariantCulture)}",
        };

        if (observation.ExhaustedAtLimitBytes is { } exhaustedAt)
        {
            lines.Add($"{ExhaustedAtKey}={exhaustedAt.ToString(CultureInfo.InvariantCulture)}");
        }

        if (observation.OverriddenAtLimitBytes is { } overriddenAt)
        {
            lines.Add($"{OverriddenAtKey}={overriddenAt.ToString(CultureInfo.InvariantCulture)}");
        }

        return string.Join(Environment.NewLine, lines) + Environment.NewLine;
    }

    /// <summary>
    /// Writes an observation, replacing any previous one.
    /// </summary>
    /// <remarks>
    /// Returns a flag rather than throwing, because every caller is on a path where
    /// failing to record a diagnostic must not become a failure to run. The return
    /// value exists so a caller can log the loss rather than be unaware of it.
    /// </remarks>
    /// <param name="path">The history file's path.</param>
    /// <param name="observation">The observation to record.</param>
    /// <returns><see langword="true"/> when the record was written.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is null.</exception>
    public static bool TryWrite(string path, RepoContextMemoryObservation observation)
    {
        ArgumentNullException.ThrowIfNull(path);

        try
        {
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(path, Render(observation));
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException or ArgumentException)
        {
            return false;
        }
    }

    private static bool TryReadRequiredBytes(
        IReadOnlyDictionary<string, string> fields,
        string key,
        out long value)
    {
        value = 0;
        return fields.TryGetValue(key, out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out value)
            && value >= 0;
    }

    private static bool TryReadOptionalBytes(
        IReadOnlyDictionary<string, string> fields,
        string key,
        out long? value)
    {
        value = null;
        if (!fields.TryGetValue(key, out var raw))
        {
            return true;
        }

        if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed < 0)
        {
            return false;
        }

        value = parsed;
        return true;
    }
}
