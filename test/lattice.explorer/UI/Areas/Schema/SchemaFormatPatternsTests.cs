using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

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
            Assert.That(pattern, Does.StartWith("^").And.EndWith("\\z"));
            Assert.That(SchemaPatterns.TryCompile(pattern, out _, out var error), Is.True, error);
            Assert.That(SchemaFormatPatterns.TryRecognise(pattern, out var recognised), Is.True);
            Assert.That(recognised, Is.EqualTo(format));
        });
    }

    /// <summary>
    /// Regression: the tail anchor used to be <c>$</c>, which in .NET also
    /// matches immediately before a line feed that ends the input, so every
    /// format accepted its own passing example with a newline smuggled onto the
    /// end. These patterns compile to a cluster-enforced regex rule applied to
    /// the raw write payload, so that was a validation bypass and not a display
    /// quirk.
    /// </summary>
    /// <param name="name">The format under test.</param>
    [TestCaseSource(nameof(Formats))]
    public void Each_format_refuses_its_example_with_a_trailing_newline(string name)
    {
        var format = Enum.Parse<SchemaTextFormat>(name);
        var passing = SchemaFormatPatterns.PassingExampleOf(format);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormatPatterns.IsMatch(format, passing), Is.True, "the example itself still passes");
            Assert.That(SchemaFormatPatterns.IsMatch(format, passing + "\n"), Is.False, "trailing line feed");
            Assert.That(SchemaFormatPatterns.IsMatch(format, passing + "\r\n"), Is.False, "trailing carriage return and line feed");
            Assert.That(SchemaFormatPatterns.IsMatch(format, passing + "\n\n"), Is.False, "two trailing line feeds");
        });
    }

    /// <summary>
    /// A policy saved before the anchor changed carries the <c>$</c> form.
    /// Recognising it as its format card would show a card whose save silently
    /// rewrote the stored pattern, so it is deliberately not recognised: the
    /// builder shows the real pattern and the operator chooses the format card,
    /// which writes the <c>\z</c> form.
    /// </summary>
    /// <param name="name">The format under test.</param>
    [TestCaseSource(nameof(Formats))]
    public void A_legacy_dollar_anchored_pattern_is_not_recognised_as_its_format(string name)
    {
        var format = Enum.Parse<SchemaTextFormat>(name);
        var pattern = SchemaFormatPatterns.PatternOf(format);
        var legacy = string.Concat(pattern.AsSpan(0, pattern.Length - 2), "$");

        Assert.Multiple(() =>
        {
            Assert.That(pattern, Does.EndWith("\\z"));
            Assert.That(SchemaFormatPatterns.TryRecognise(legacy, out _), Is.False, legacy);
            Assert.That(SchemaFormatPatterns.TryRecognise(pattern, out var recognised), Is.True);
            Assert.That(recognised, Is.EqualTo(format));
        });
    }

    [TestCase("user.name+tag@mail.example.co.uk", true)]
    [TestCase("a@b", false)]
    [TestCase("a b@example.com", false)]
    public void Email(string text, bool expected) => Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.Email, text), Is.EqualTo(expected));

    [TestCase("1.2.3-01", false)]
    [TestCase("1.2.3-alpha.01", false)]
    [TestCase("1.2.3-.", false)]
    [TestCase("1.2.3-alpha..1", false)]
    [TestCase("1.2.3-alpha.", false)]
    [TestCase("1.2.3+.", false)]
    [TestCase("1.2.3+build..1", false)]
    [TestCase("1.2.3+build.", false)]
    [TestCase("1.2.3-0", true)]
    [TestCase("1.2.3-alpha.0.rc-1+build.001", true)]
    [TestCase("1.2.3-01a", true)]
    [TestCase("1.2.3--", true)]
    public void Semantic_version_card_enforces_identifier_rules_in_the_compiled_policy(string text, bool expected)
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.Format, "version");
        card.Format = SchemaTextFormat.SemanticVersion;

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        var sample = new SchemaSample("t", null,
            [new SchemaSampleValue("k", System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { version = text }))], 0, false);
        var preview = SchemaPreviewResult.Evaluate([rule], sample);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormatPatterns.IsMatch(SchemaTextFormat.SemanticVersion, text), Is.EqualTo(expected));
            Assert.That(preview.Passed, Is.EqualTo(expected ? 1 : 0));
        });
    }

    [Test]
    public void Semantic_version_legacy_policy_reopens_without_silently_changing_its_pattern()
    {
        const string legacy = "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\\+[0-9A-Za-z.-]+)?\\z";
        var original = LatticeSchemaRule.Regex(legacy, "version");
        var card = SchemaCardDecompiler.Decompile(original);

        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.Pattern));
            Assert.That(card.Pattern, Is.EqualTo(legacy));
            Assert.That(SchemaCardCompiler.TryCompile(card, out var recompiled, out var error), Is.True, error);
            Assert.That(recompiled, Is.EqualTo(original));
        });
    }

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
    [TestCase("EndsWith", "(x)", "\\(x\\)\\z")]
    [TestCase("Contains", "1+1", "1\\+1")]
    public void A_text_match_becomes_an_escaped_pattern(string match, string text, string pattern)
    {
        Assert.That(SchemaPatterns.ForMatch(Enum.Parse<SchemaTextMatch>(match), text), Is.EqualTo(pattern));
    }

    /// <summary>
    /// Regression: an "ends with" card taken over as a regex used to be anchored
    /// with <c>$</c>, so the enforced pattern accepted the text followed by a
    /// newline - which <see cref="string.EndsWith(string, StringComparison)"/>,
    /// the behaviour the card claims, does not.
    /// </summary>
    [Test]
    public void An_ends_with_pattern_refuses_a_trailing_newline()
    {
        var regex = SchemaPatterns.Compile(SchemaPatterns.ForMatch(SchemaTextMatch.EndsWith, "-final"));

        Assert.Multiple(() =>
        {
            Assert.That(regex.IsMatch("order-final"), Is.True);
            Assert.That(regex.IsMatch("order-final\n"), Is.False);
            Assert.That(regex.IsMatch("order-final\r\n"), Is.False);
            Assert.That(regex.IsMatch("order-final-x"), Is.False);
        });
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
