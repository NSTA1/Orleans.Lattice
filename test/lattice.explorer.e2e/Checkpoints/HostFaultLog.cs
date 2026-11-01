using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// The estate's host-fault log: the error and critical records each estate process has
/// written to its captured log since the previous checkpoint, read incrementally so every
/// record is attributed to exactly one checkpoint (the first one after it was written).
/// </summary>
/// <remarks>
/// A record is a console-logger entry at <c>fail:</c> or <c>crit:</c> level, with the
/// indented lines that follow it (its message and exception), as the journey hosts write
/// them. Shared by every session of an estate, so two sessions never report one record twice.
/// </remarks>
public sealed partial class HostFaultLog
{
    private const int MaxContinuationLines = 12;

    /// <summary>
    /// Records the product writes on purpose and that are known not to be faults, each matched
    /// on its exact logger category and message shape and cited to the issue that tracks it. One
    /// of these is reported as expected, never as a failure. Add an entry only with an issue,
    /// and remove it when the issue is fixed.
    /// </summary>
    internal static IReadOnlyList<KnownHostRecord> Expected { get; } = [];

    private readonly Func<IEnumerable<HostLogSource>> _sources;
    private readonly Dictionary<string, long> _offsets = new(StringComparer.Ordinal);
    private readonly List<Regex> _tolerated = [];
    private readonly Lock _gate = new();

    /// <summary>Reads the logs <paramref name="sources"/> names, asked afresh at every checkpoint so a process started later is read too.</summary>
    /// <param name="sources">The processes whose logs to read.</param>
    public HostFaultLog(Func<IEnumerable<HostLogSource>> sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        _sources = sources;
    }

    /// <summary>Tolerates, from now on, every record matching <paramref name="pattern"/>: it is reported, but fails nothing.</summary>
    /// <param name="pattern">The pattern, matched against the record's text.</param>
    public void Tolerate(Regex pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        lock (_gate)
        {
            _tolerated.Add(pattern);
        }
    }

    /// <summary>Takes every record written since the last call: those that fail the checkpoint, and those only reported.</summary>
    public (IReadOnlyList<string> Failures, IReadOnlyList<string> Notes) Drain()
    {
        var failures = new List<string>();
        var notes = new List<string>();
        lock (_gate)
        {
            foreach (var source in _sources())
            {
                foreach (var record in ReadNewRecords(source))
                {
                    var line = $"[{source.Name}] {record}";
                    if (Expected.FirstOrDefault(known => known.Pattern.IsMatch(record)) is { } expected)
                    {
                        notes.Add($"[{source.Name}] expected ({expected.Issue}): {FirstLine(record)}");
                    }
                    else if (source.FailsCheckpoint && !_tolerated.Any(pattern => pattern.IsMatch(record)))
                    {
                        failures.Add(line);
                    }
                    else
                    {
                        notes.Add(line);
                    }
                }
            }
        }

        return (failures, notes);
    }

    private List<string> ReadNewRecords(HostLogSource source)
    {
        var records = new List<string>();
        if (!File.Exists(source.LogFile))
        {
            return records;
        }

        _offsets.TryGetValue(source.LogFile, out var offset);
        byte[] bytes;
        using (var stream = new FileStream(source.LogFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            if (stream.Length < offset)
            {
                offset = 0;
            }

            stream.Seek(offset, SeekOrigin.Begin);
            bytes = new byte[stream.Length - offset];
            stream.ReadExactly(bytes);
        }

        // Only whole lines: a line still being written is read at the next checkpoint.
        var complete = Array.LastIndexOf(bytes, (byte)'\n') + 1;
        _offsets[source.LogFile] = offset + complete;
        if (complete == 0)
        {
            return records;
        }

        StringBuilder? current = null;
        var continuation = 0;
        foreach (var raw in Encoding.UTF8.GetString(bytes, 0, complete).Split('\n'))
        {
            var text = Payload(raw.TrimEnd('\r'));
            if (FaultStart().IsMatch(text))
            {
                if (current is not null)
                {
                    records.Add(current.ToString());
                }

                current = new StringBuilder(text.Trim());
                continuation = 0;
            }
            else if (current is not null && text.Length > 0 && char.IsWhiteSpace(text[0]))
            {
                if (continuation++ < MaxContinuationLines)
                {
                    current.Append(Environment.NewLine).Append("    ").Append(text.Trim());
                }
            }
            else if (current is not null)
            {
                records.Add(current.ToString());
                current = null;
            }
        }

        if (current is not null)
        {
            records.Add(current.ToString());
        }

        return records;
    }

    private static string FirstLine(string record)
    {
        var end = record.IndexOf('\n', StringComparison.Ordinal);
        return (end < 0 ? record : record[..end]).TrimEnd('\r');
    }

    // The harness prefixes every captured line with its time and stream: "12:00:00.000 [out] ".
    private static string Payload(string line)
    {
        var match = CapturedLine().Match(line);
        return match.Success ? match.Groups["text"].Value : line;
    }

    [GeneratedRegex(@"^\d{2}:\d{2}:\d{2}\.\d{3} \[(out|err|harness)\] (?<text>.*)$")]
    private static partial Regex CapturedLine();

    [GeneratedRegex(@"^(fail|crit): ")]
    private static partial Regex FaultStart();
}
