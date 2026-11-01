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

        if (IsExplorerPolicyViolation(text, location))
        {
            Add(fault: true, $"[csp {location}] {text}");
        }
        else if (message.Type == "error")
        {
            Add(fault: true, $"[console.error {location}] {text}");
        }
        else if (message.Type == "warning")
        {
            Add(fault: false, $"[console.warning {location}] {text}");
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

    // As the UI-test suite reads it: a violation reported by one of the Explorer's own
    // documents, not by an app's sandboxed frame, whose own policy is the app's business.
    private static bool IsExplorerPolicyViolation(string text, string location) =>
        Regex.IsMatch(text, "Content[- ]Security[- ]Policy", RegexOptions.IgnoreCase)
        && !Regex.IsMatch(text, "Unrecognized|unknown directive|Couldn.t process|not supported", RegexOptions.IgnoreCase)
        && !location.Contains("/_apps/frame/", StringComparison.OrdinalIgnoreCase)
        && !location.StartsWith("blob:", StringComparison.OrdinalIgnoreCase);
}
