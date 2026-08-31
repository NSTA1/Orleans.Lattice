using System.Text.Json;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Runs a JavaScript accessibility probe in the page and returns its report.
/// <para>
/// Every probe returns the same shape - how many candidate elements it examined, and
/// a list of human-readable problems - so each one carries its own vacuity guard.
/// A probe that examined nothing has proved nothing, and without that count a
/// structural check reports the same empty problem list for "the contract holds" and
/// for "the elements were never there", which is the identical false-pass trap that
/// makes an axe sweep of a blank page report zero violations.
/// </para>
/// </summary>
internal static class AccessibilityProbe
{
    /// <summary>A probe's report: what it looked at, and what was wrong.</summary>
    /// <param name="Examined">How many candidate elements the probe evaluated.</param>
    /// <param name="Problems">One line per violated expectation; empty when clean.</param>
    internal readonly record struct Report(int Examined, IReadOnlyList<string> Problems)
    {
        /// <summary>The problems as a single message, one per line.</summary>
        public override string ToString() => string.Join(Environment.NewLine, Problems);
    }

    /// <summary>
    /// Evaluates <paramref name="script"/> - which must return
    /// <c>{ examined: number, problems: string[] }</c> - and marshals the result.
    /// </summary>
    /// <param name="page">The page to evaluate in.</param>
    /// <param name="script">The probe script.</param>
    /// <param name="argument">An optional argument passed to the probe.</param>
    internal static async Task<Report> RunAsync(IPage page, string script, object? argument = null)
    {
        var raw = await page.EvaluateAsync<JsonElement>(script, argument);
        var examined = raw.GetProperty("examined").GetInt32();
        var problemsJson = raw.GetProperty("problems");
        var count = problemsJson.GetArrayLength();

        // The clean case is the common one - the focus walk alone runs this thirty times
        // per test - so it costs the shared empty singleton rather than a fresh
        // zero-length array each time.
        if (count == 0)
        {
            return new Report(examined, Array.Empty<string>());
        }

        var problems = new string[count];
        var index = 0;
        foreach (var problem in problemsJson.EnumerateArray())
        {
            problems[index++] = problem.GetString() ?? string.Empty;
        }

        return new Report(examined, problems);
    }

    /// <summary>
    /// Asserts the landmark structure of the surface currently rendered in
    /// <paramref name="page"/>.
    /// <para>
    /// Shared between the structural fixture, which checks it on the home surface at
    /// every breakpoint band, and the sweep, which re-checks it after every area
    /// activation - because the shell wraps its own home surface in a <c>main</c> but
    /// renders an active area plugin directly, so the working surface can lose its
    /// landmark the moment the user leaves home. Folding the check into the area walk
    /// means it covers exactly the areas the harness can reach, and starts covering a
    /// plugin surface automatically on the day one becomes reachable, rather than
    /// standing as a case that can never run.
    /// </para>
    /// </summary>
    /// <param name="page">The page whose current surface to check.</param>
    /// <param name="surface">A description of the surface, for the failure message.</param>
    internal static async Task AssertLandmarksAsync(IPage page, string surface)
    {
        var report = await RunAsync(page, Landmarks);

        Assert.That(report.Problems, Is.Empty, () =>
            $"The landmark structure of {surface} does not satisfy WCAG SC 1.3.1 Info and "
            + $"Relationships / SC 2.4.1 Bypass Blocks ({report.Examined} landmarks found)."
            + Environment.NewLine + report);
    }

    /// <summary>
    /// Counts the shell's landmarks and reports the ones a surface must have and does
    /// not.
    /// </summary>
    private const string Landmarks =
        """
        () => {
            const problems = [];
            const mains = document.querySelectorAll('main, [role=main]');
            const navigations = document.querySelectorAll('nav, [role=navigation]');

            // A bare <header> is a banner landmark only when it is not scoped inside a
            // sectioning element, which is why the plugin panels' own headers do not count.
            const banners = Array.from(document.querySelectorAll('header'))
                .filter(el => el.closest('main, nav, section, article, aside') === null)
                .concat(Array.from(document.querySelectorAll('[role=banner]')));

            if (mains.length !== 1) {
                problems.push('the surface exposes ' + mains.length + ' main landmarks; exactly one is '
                    + 'required so a skip link and a landmark jump have a single unambiguous destination');
            }

            if (navigations.length === 0) {
                problems.push('the surface exposes no navigation landmark, so assistive technology cannot '
                    + 'jump to the navigation');
            }

            if (banners.length === 0) {
                problems.push('the surface exposes no banner landmark for the shell chrome');
            }

            return { examined: mains.length + navigations.length + banners.length, problems };
        }
        """;
}
