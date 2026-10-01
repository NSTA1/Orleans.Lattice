using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Issue #4148: a field that takes a date, a time or a duration is never a bare text box.
/// An instant is an <c>LtDateTimeInput</c> (a calendar and time picker over a typed UTC
/// entry, the zone always shown) and a duration is an <c>LtDurationInput</c> (a whole-number
/// box per unit). This scans every <c>LtTextInput</c> in the Explorer's markup and fails
/// when its label, hint or placeholder says it takes one - a date, a time, UTC, ISO, an
/// interval, a window or a count of seconds, minutes, hours, days or weeks.
/// </summary>
[TestFixture]
public sealed class ShellTimeFieldHygieneTests
{
    private const string ShellSourceRoot = "src/lattice.explorer/UI";

    /// <summary>A text input, from its tag to its self-closing end, across lines.</summary>
    private static readonly Regex TextInput = new(@"<LtTextInput\b(?<attributes>.*?)/>", RegexOptions.Compiled | RegexOptions.Singleline);

    /// <summary>The attributes whose text says what a field takes.</summary>
    private static readonly Regex Described = new("\\b(?<name>Label|Hint|Placeholder)=\"(?<text>[^\"]*)\"", RegexOptions.Compiled);

    /// <summary>The words that say a field takes a date, a time or a duration.</summary>
    private static readonly Regex Temporal = new(
        @"\b(?:dates?|times?|timestamps?|datetime|utc|gmt|iso(?:\s*8601)?|durations?|intervals?|windows?|timeouts?|ttl|expir\w*|as\s+of|until|since|seconds?|minutes?|hours?|days?|weeks?|months?|years?|yyyy|hh:mm)\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    [Test]
    public void No_date_time_or_duration_field_is_a_bare_text_input()
    {
        var inputs = 0;
        var violations = new List<string>();
        var root = ShellStylesheets.Absolute(ShellSourceRoot);
        foreach (var file in HygieneRepository.EnumerateFiles(root, "*.razor"))
        {
            var relative = Path.GetRelativePath(HygieneRepository.FindRepoRoot(), file).Replace('\\', '/');
            var markup = Regex.Replace(File.ReadAllText(file), @"@\*.*?\*@|<!--.*?-->", string.Empty, RegexOptions.Singleline);
            foreach (var violation in Violations(markup, ref inputs))
            {
                violations.Add($"{relative}: {violation}");
            }
        }

        Assert.That(inputs, Is.GreaterThan(30), "the scan must reach the Explorer's text inputs");
        Assert.That(violations, Is.Empty,
            "A field that takes a date or a time is an LtDateTimeInput, and one that takes a duration is an LtDurationInput, "
            + "never a bare LtTextInput:" + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_scanner_finds_a_time_field_drawn_as_text_and_passes_one_that_is_not()
    {
        // Battery test for the smoke detector.
        var inputs = 0;
        const string markup =
            """
            <LtTextInput Label="As of (UTC)" Mono="true" Value="@_at" ValueChanged="value => _at = value" />
            <LtTextInput Label="Every"
                         Hint="In whole minutes."
                         Value="@_every" />
            <LtTextInput Label="Retention" Placeholder="yyyy-MM-dd" />
            <LtTextInput Label="Window in seconds" />
            <LtTextInput Label="Key prefix" Mono="true" Hint="Keys that start with this." />
            <LtTextInput Label="Largest size, in bytes" inputmode="numeric" />
            <LtDateTimeInput Label="As of" />
            """;

        var found = Violations(markup, ref inputs).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(inputs, Is.EqualTo(6), "every text input, each counted once, the multi-line one included");
            Assert.That(found, Has.Length.EqualTo(4));
            Assert.That(found[0], Does.Contain("As of (UTC)"));
            Assert.That(found[1], Does.Contain("In whole minutes."), "a hint names a duration as surely as a label");
            Assert.That(found[2], Does.Contain("yyyy-MM-dd"), "a placeholder in a date's form is a date field");
            Assert.That(found[3], Does.Contain("Window in seconds"));
        });
    }

    private static IEnumerable<string> Violations(string markup, ref int inputs)
    {
        var found = new List<string>();
        foreach (Match input in TextInput.Matches(markup))
        {
            inputs++;
            foreach (Match attribute in Described.Matches(input.Groups["attributes"].Value))
            {
                var text = attribute.Groups["text"].Value;
                if (Temporal.Match(text) is { Success: true } word)
                {
                    found.Add($"an LtTextInput whose {attribute.Groups["name"].Value} \"{text}\" says it takes \"{word.Value}\"");
                    break;
                }
            }
        }

        return found;
    }
}
