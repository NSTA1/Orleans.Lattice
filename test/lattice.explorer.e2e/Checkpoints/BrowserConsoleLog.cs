using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// What a browser page has said since the last checkpoint: console errors and warnings,
/// uncaught page errors, and the Content-Security-Policy violations the Explorer's own
/// documents report. A checkpoint fails on any error or violation not tolerated.
/// </summary>
public sealed class BrowserConsoleLog
{
    private readonly List<string> _failures = [];
    private readonly List<string> _notes = [];
    private readonly List<Regex> _tolerated = [];
    private readonly Lock _gate = new();

    /// <summary>Starts recording what <paramref name="page"/> says.</summary>
    /// <param name="page">The page.</param>
    public void Attach(IPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        page.Console += (_, message) => OnConsole(message);
        page.PageError += (_, error) => Add(fault: true, $"[pageerror] {error}");
    }

    /// <summary>Tolerates, from now on, every console or page error matching <paramref name="pattern"/>.</summary>
    /// <param name="pattern">The pattern, matched against the recorded line.</param>
    public void Tolerate(Regex pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        lock (_gate)
        {
            _tolerated.Add(pattern);
        }
    }

    /// <summary>Takes what was recorded since the last call: the failures, and the warnings noted.</summary>
    public (IReadOnlyList<string> Failures, IReadOnlyList<string> Notes) Drain()
    {
        lock (_gate)
        {
            IReadOnlyList<string> failures = _failures.ToArray();
            IReadOnlyList<string> notes = _notes.ToArray();
            _failures.Clear();
            _notes.Clear();
            return (failures, notes);
        }
    }

    private void OnConsole(IConsoleMessage message)
    {
        // Read defensively: this runs inside a Playwright event callback, where an exception
        // ends the test host.
        string text;
        string location;
        try
        {
            text = message.Text;
            location = message.Location ?? string.Empty;
        }
        catch (Exception)
        {
            return;
        }

        switch (Classify(message.Type, text, location))
        {
            case ConsoleLine.PolicyViolation:
                Add(fault: true, $"[csp {location}] {text}");
                break;
            case ConsoleLine.Error:
                Add(fault: true, $"[console.error {location}] {text}");
                break;
            case ConsoleLine.Note:
                Add(fault: false, $"[console.{message.Type} {location}] {text}");
                break;
        }
    }

    private void Add(bool fault, string line)
    {
        lock (_gate)
        {
            if (fault && !_tolerated.Any(pattern => pattern.IsMatch(line)))
            {
                _failures.Add(line);
            }
            else
            {
                _notes.Add(line);
            }
        }
    }

    /// <summary>How a console line counts at a checkpoint.</summary>
    internal enum ConsoleLine
    {
        /// <summary>Not recorded: neither an error nor a warning.</summary>
        Ignored,

        /// <summary>Recorded, failing nothing.</summary>
        Note,

        /// <summary>A console error: fails the checkpoint.</summary>
        Error,

        /// <summary>A Content-Security-Policy violation by one of the Explorer's own documents: fails the checkpoint.</summary>
        PolicyViolation,
    }

    /// <summary>
    /// Classifies a console line. A Content-Security-Policy report is the Explorer's business only
    /// when one of its own documents makes it: an app's sandboxed frame enforces its own policy, and
    /// what that policy refuses inside the frame (the frame's documents report it with their own
    /// <c>/_apps/frame/</c> location, a <c>blob:</c> one, or - from the opaque-origin sandbox - no
    /// URL at all while naming a <c>/_apps/frame/</c> resource) is the app's. A directive the browser
    /// does not recognise, such as <c>webrtc</c>, is reported but fails nothing: the policy names it
    /// on purpose for the browsers that enforce it.
    /// </summary>
    /// <param name="type">The console message type.</param>
    /// <param name="text">The message text.</param>
    /// <param name="location">The message location.</param>
    internal static ConsoleLine Classify(string type, string text, string location)
    {
        if (Regex.IsMatch(text, "Content[- ]Security[- ]Policy", RegexOptions.IgnoreCase))
        {
            var noise = Regex.IsMatch(text, "Unrecognized|unknown directive|Couldn.t process|not supported", RegexOptions.IgnoreCase);
            var frame = location.Contains("/_apps/frame/", StringComparison.OrdinalIgnoreCase)
                || location.StartsWith("blob:", StringComparison.OrdinalIgnoreCase)
                || (!location.Contains("://", StringComparison.Ordinal) && text.Contains("/_apps/frame/", StringComparison.OrdinalIgnoreCase));
            return noise || frame ? ConsoleLine.Note : ConsoleLine.PolicyViolation;
        }

        return type switch
        {
            "error" => ConsoleLine.Error,
            "warning" => ConsoleLine.Note,
            _ => ConsoleLine.Ignored,
        };
    }
}
