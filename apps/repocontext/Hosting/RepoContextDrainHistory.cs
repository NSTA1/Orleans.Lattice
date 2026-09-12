using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The durable record of the last drain, carried across a restart on the container's
/// data mount so the next process can relate the budget it derives to the drain that
/// budget actually has to cover.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a file and not a metric.</b> The measurement this store carries is taken
/// during shutdown, at the point where the scrape endpoint is closing and the
/// orchestrator has already stopped polling. A metric emitted then is not read by
/// anything. The only consumer that reliably exists after a drain is the next
/// process, and the only channel that reaches it is the data mount both processes
/// share.
/// </para>
/// <para>
/// <b>It is diagnostics and it fails soft, in both directions.</b> A read that
/// cannot parse the file returns <see langword="null"/> - the same as no history -
/// rather than throwing, because a corrupt diagnostic must never stop a container
/// starting. A write that fails is swallowed for the stricter version of the same
/// reason: it runs on the shutdown path, where throwing would cost the remainder of
/// the stop sequence to save a record of it.
/// </para>
/// <para>
/// <b>The format is deliberately line-based text rather than JSON.</b> It is written
/// by a process that is shutting down and read by one that is starting, so both ends
/// want the cheapest possible parse with no serializer configuration to get wrong;
/// and the image is distroless, so the file is read by mounting the volume, where
/// <c>key=value</c> lines survive being looked at by eye. An unrecognised key is
/// ignored and a missing key reads as absent, so a field added later is
/// backward-compatible without a version negotiation.
/// </para>
/// </remarks>
public static class RepoContextDrainHistory
{
    /// <summary>The file name written under the container's data directory.</summary>
    public const string FileName = "drain-history.txt";

    /// <summary>
    /// The format version written into the file. A file declaring a version this
    /// build does not recognise is treated as absent rather than guessed at.
    /// </summary>
    public const int FormatVersion = 1;

    private const string VersionKey = "version";
    private const string ObservedAtKey = "observedAtUtc";
    private const string OutcomeKey = "outcome";
    private const string BudgetKey = "budgetSeconds";
    private const string DurationKey = "durationSeconds";
    private const string ResidentKey = "residentActivations";

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
    /// Reads the last recorded drain, or <see langword="null"/> when there is no
    /// readable record.
    /// </summary>
    /// <param name="path">The history file's path.</param>
    /// <returns>The recorded observation, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is null.</exception>
    public static RepoContextDrainObservation? Read(string path)
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
    /// touching the file system, and so a caller that has the content already does
    /// not have to round-trip it through a file to read it.
    /// </summary>
    /// <param name="lines">The file's lines.</param>
    /// <returns>The recorded observation, or <see langword="null"/> when the content is not a readable record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="lines"/> is null.</exception>
    public static RepoContextDrainObservation? Parse(IEnumerable<string> lines)
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
            || !Enum.TryParse<RepoContextDrainOutcome>(rawOutcome, ignoreCase: true, out var outcome)
            || !Enum.IsDefined(outcome))
        {
            return null;
        }

        if (!TryReadSeconds(fields, BudgetKey, out var budget) || budget is null)
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

        if (!TryReadSeconds(fields, DurationKey, out var duration))
        {
            return null;
        }

        int? resident = null;
        if (fields.TryGetValue(ResidentKey, out var rawResident))
        {
            if (!int.TryParse(rawResident, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
                || parsed < 0)
            {
                return null;
            }

            resident = parsed;
        }

        return new RepoContextDrainObservation(observedAt, outcome, budget.Value, duration, resident);
    }

    /// <summary>
    /// Renders an observation in the file's format. Exposed so the round trip is
    /// testable without a file system, and so a caller can log exactly what was
    /// written.
    /// </summary>
    /// <param name="observation">The observation to render.</param>
    /// <returns>The file content.</returns>
    public static string Render(RepoContextDrainObservation observation)
    {
        var lines = new List<string>(6)
        {
            $"{VersionKey}={FormatVersion.ToString(CultureInfo.InvariantCulture)}",
            $"{ObservedAtKey}={observation.ObservedAtUtc.ToUniversalTime():O}",
            $"{OutcomeKey}={observation.Outcome}",
            $"{BudgetKey}={observation.Budget.TotalSeconds.ToString("R", CultureInfo.InvariantCulture)}",
        };

        if (observation.Duration is { } duration)
        {
            lines.Add($"{DurationKey}={duration.TotalSeconds.ToString("R", CultureInfo.InvariantCulture)}");
        }

        if (observation.ResidentActivations is { } resident)
        {
            lines.Add($"{ResidentKey}={resident.ToString(CultureInfo.InvariantCulture)}");
        }

        return string.Join(Environment.NewLine, lines) + Environment.NewLine;
    }

    /// <summary>
    /// Writes an observation, replacing any previous one.
    /// </summary>
    /// <remarks>
    /// Returns a flag rather than throwing because every caller is on a path where a
    /// failure to record diagnostics must not become a failure to shut down. The
    /// return value exists so a caller can log the loss rather than be unaware of it.
    /// </remarks>
    /// <param name="path">The history file's path.</param>
    /// <param name="observation">The observation to record.</param>
    /// <returns><see langword="true"/> when the record was written.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is null.</exception>
    public static bool TryWrite(string path, RepoContextDrainObservation observation)
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

    private static bool TryReadSeconds(
        IReadOnlyDictionary<string, string> fields,
        string key,
        out TimeSpan? value)
    {
        value = null;
        if (!fields.TryGetValue(key, out var raw))
        {
            return true;
        }

        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds)
            || double.IsNaN(seconds)
            || double.IsInfinity(seconds)
            || seconds < 0d)
        {
            return false;
        }

        value = TimeSpan.FromSeconds(seconds);
        return true;
    }
}
