using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The regular expressions behind the format and text-match cards: every format
/// accepts its passing example, refuses its failing one, runs on the cluster's
/// linear-time engine and reads back as itself; a text match is escaped exactly.
/// </summary>
[TestFixture]
public sealed class SchemaFormatPatternsTests
{
    private static IEnumerable<string> Formats() => SchemaFormatPatterns.All.Select(format => format.ToString());

    [TestCaseSource(nameof(Formats))]
    public void Each_format_accepts_its_example_and_refuses_its_counter_example(string name)
    {
        var format = Enum.Parse<SchemaTextFormat>(name);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormatPatterns.IsMatch(format, SchemaFormatPatterns.PassingExampleOf(format)), Is.True, "passing example");
            Assert.That(SchemaFormatPatterns.IsMatch(format, SchemaFormatPatterns.FailingExampleOf(format)), Is.False, "failing example");
            Assert.That(SchemaFormatPatterns.NameOf(format), Is.Not.Empty);
            Assert.That(SchemaFormatPatterns.PhraseOf(format), Is.Not.Empty);
        });
    }

    [TestCaseSource(nameof(Formats))]
    public void Each_format_is_anchored_runs_in_linear_time_and_reads_back_as_itself(string name)
    {
        var format = Enum.Parse<SchemaTextFormat>(name);
        var pattern = SchemaFormatPatterns.PatternOf(format);

        Assert.Multiple(() =>
        {
            Assert.That(pattern, Does.StartWith("^").And.EndWith("$"));
            Assert.That(SchemaPatterns.TryCompile(pattern, out _, out var error), Is.True, error);
            Assert.That(SchemaFormatPatterns.TryRecognise(pattern, out var recognised), Is.True);
            Assert.That(recognised, Is.EqualTo(format));
        });
    }

    [TestCase("user.name+tag@mail.example.co.uk", true)]
    [TestCase("a@b", false)]
    [TestCase("a b@example.com", false)]
    public void Email(string text, bool expected) => Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.Email, text), Is.EqualTo(expected));

    [TestCase("2026-02-30", true)]
    [TestCase("2026-13-01", false)]
    [TestCase("2026-1-01", false)]
    public void Date_checks_the_shape_not_the_calendar(string text, bool expected) =>
        Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.Date, text), Is.EqualTo(expected));

    [TestCase("2026-09-29T14:02:11.123+01:00", true)]
    [TestCase("2026-09-29T14:02Z", true)]
    [TestCase("2026-09-29T14:02:11", false)]
    public void Date_time_needs_an_offset(string text, bool expected) =>
        Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.DateTime, text), Is.EqualTo(expected));

    [TestCase("::1", true)]
    [TestCase("::", true)]
    [TestCase("fe80::1:2:3:4", true)]
    [TestCase("2001:0db8:0000:0000:0000:ff00:0042:8329", true)]
    [TestCase("1:2:3:4:5:6:7:8:9", false)]
    [TestCase("12345::1", false)]
    public void Ipv6(string text, bool expected) => Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.Ipv6, text), Is.EqualTo(expected));

    [TestCase("0.0.0.0", true)]
    [TestCase("255.255.255.255", true)]
    [TestCase("256.1.1.1", false)]
    [TestCase("1.1.1", false)]
    public void Ipv4(string text, bool expected) => Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.Ipv4, text), Is.EqualTo(expected));

    [Test]
    public void A_pattern_no_format_writes_is_not_recognised()
    {
        Assert.That(SchemaFormatPatterns.TryRecognise("^[a-z]+$", out _), Is.False);
        Assert.That(SchemaFormatPatterns.TryRecognise(null, out _), Is.False);
    }

    [Test]
    public void The_formats_all_texts_match_are_found_best_first()
    {
        Assert.That(SchemaFormatPatterns.Matching(["EUR", "GBP"]), Is.EqualTo(new[] { SchemaTextFormat.CurrencyCode }));
        Assert.That(SchemaFormatPatterns.Matching(["abc"]), Does.Contain(SchemaTextFormat.Slug));
        Assert.That(SchemaFormatPatterns.Matching([]), Is.Empty);
        Assert.That(() => SchemaFormatPatterns.Matching(null!), Throws.ArgumentNullException);
    }

    [TestCase("StartsWith", "a.b", "^a\\.b")]
    [TestCase("EndsWith", "(x)", "\\(x\\)$")]
    [TestCase("Contains", "1+1", "1\\+1")]
    public void A_text_match_becomes_an_escaped_pattern(string match, string text, string pattern)
    {
        Assert.That(SchemaPatterns.ForMatch(Enum.Parse<SchemaTextMatch>(match), text), Is.EqualTo(pattern));
    }

    [Test]
    public void A_text_match_pattern_agrees_with_the_string_method()
    {
        var regex = SchemaPatterns.Compile(SchemaPatterns.ForMatch(SchemaTextMatch.StartsWith, "SKU-*"));

        Assert.That(regex.IsMatch("SKU-*12"), Is.True);
        Assert.That(regex.IsMatch("SKU-12"), Is.False);
        Assert.That(() => SchemaPatterns.ForMatch(SchemaTextMatch.Contains, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_pattern_the_cluster_cannot_run_is_refused_with_a_reason()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaPatterns.TryCompile("(a)\\1", out _, out var backReference), Is.False);
            Assert.That(backReference, Does.StartWith("The cluster cannot run this pattern"));
            Assert.That(SchemaPatterns.TryCompile("(?=a)a", out _, out _), Is.False, "look-arounds are not linear");
            Assert.That(SchemaPatterns.TryCompile("[", out _, out _), Is.False);
            Assert.That(SchemaPatterns.TryCompile(string.Empty, out _, out var empty), Is.False);
            Assert.That(empty, Is.EqualTo("Enter the pattern values must match."));
            Assert.That(SchemaPatterns.TryCompile(new string('a', SchemaPatterns.MaximumPatternLength + 1), out _, out var tooLong), Is.False);
            Assert.That(tooLong, Does.StartWith("A pattern may be at most"));
        });
    }
}
