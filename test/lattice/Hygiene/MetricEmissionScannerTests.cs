using System.IO;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Unit tests for <see cref="MetricEmissionScanner"/>'s instrument-scoping and
/// argument-splitting rules. The scanner discovers instrument names per
/// declaring type (unioned across a type's partial-class files) rather than
/// globally, so a member that merely shares a name with an instrument declared
/// in an unrelated type is not mistaken for an emission - while a bare emission
/// in a partial of the declaring type is still caught. It then splits each
/// call's measured value from its tag arguments, which requires telling a
/// generic argument list from a relational comparison.
/// </summary>
/// <remarks>
/// Deterministic file scan over a throwaway synthetic <c>src/</c> tree; nothing
/// depends on timing, ordering, or a running cluster.
/// </remarks>
[TestFixture]
public sealed class MetricEmissionScannerTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-scanner-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(_root, "src"));
    }

    [TearDown]
    public void TearDown()
    {
        if (_root is not null && Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private void WriteSource(string relativeUnderSrc, string text)
    {
        var full = Path.Combine(_root, "src", relativeUnderSrc);
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, text);
    }

    [Test]
    public void A_genuinely_untagged_bare_emission_is_reported()
    {
        WriteSource("WidgetMetrics.cs", """
            namespace Synthetic;
            internal static class WidgetMetrics
            {
                internal static readonly Counter<long> Widgets = Meter.CreateCounter<long>("widgets");
                internal static void Emit() => Widgets.Add(1);
            }
            """);

        var sites = MetricEmissionScanner.Scan(_root);

        Assert.That(
            sites.Any(s => s.RelativePath == "src/WidgetMetrics.cs" && s.Instrument == "Widgets"),
            Is.True,
            "A bare emission of an instrument declared in the same type must still be reported.");
    }

    [Test]
    public void A_bare_name_matching_an_instrument_declared_in_another_type_is_not_an_emission_site()
    {
        // WidgetMetrics declares the instrument 'Widgets'.
        WriteSource("WidgetMetrics.cs", """
            namespace Synthetic;
            internal static class WidgetMetrics
            {
                internal static readonly Counter<long> Widgets = Meter.CreateCounter<long>("widgets");
            }
            """);

        // A different, metrics-facing type happens to hold a collection also
        // named 'Widgets' and calls .Add on it. Under global instrument
        // discovery this read as an untagged emission; per declaring type it
        // must not, because 'Widgets' is not an instrument of this type.
        WriteSource("CollisionHolder.cs", """
            namespace Synthetic;
            // Mentions Metrics so the metric-file gate is satisfied.
            internal sealed class CollisionHolder
            {
                private readonly List<int> Widgets = new();
                public void Track(int n) => Widgets.Add(n);
            }
            """);

        var sites = MetricEmissionScanner.Scan(_root);

        Assert.That(
            sites.Any(s => s.RelativePath == "src/CollisionHolder.cs"),
            Is.False,
            "A member sharing a name with an instrument declared in a different type "
            + "must not be reported as an emission site.");
    }

    [Test]
    public void A_bare_emission_in_a_partial_of_the_declaring_type_is_reported()
    {
        // The instrument is declared in one partial file...
        WriteSource("Gauge.cs", """
            namespace Synthetic;
            internal static partial class Gauge
            {
                internal static readonly Histogram<double> Beats = Meter.CreateHistogram<double>("beats");
            }
            """);

        // ...and emitted, bare, in a sibling partial following the
        // {TypeName}.{Concern}.cs convention. Per-file discovery would wrongly
        // drop this; per declaring type (which unions partials) keeps it.
        WriteSource("Gauge.Emit.cs", """
            namespace Synthetic;
            // Emits through the Meter-backed instrument declared in Gauge.cs.
            internal static partial class Gauge
            {
                internal static void Emit() => Beats.Record(1.0);
            }
            """);

        var sites = MetricEmissionScanner.Scan(_root);

        Assert.That(
            sites.Any(s => s.RelativePath == "src/Gauge.Emit.cs" && s.Instrument == "Beats"),
            Is.True,
            "A bare emission in a partial of the declaring type must still be reported.");
    }

    [Test]
    public void A_relational_less_than_in_the_first_argument_does_not_swallow_the_tag_list()
    {
        // The measured value is a clamp written with a relational comparison.
        // Read as a generic-argument open, the '<' is never balanced, the
        // argument separator is never found, and the tags read as empty - which
        // the tenant-dimension gate reports as a missing dimension against a
        // call site that supplies its tags correctly.
        WriteSource("Clamp.cs", """
            namespace Synthetic;
            internal static class Clamp
            {
                internal static readonly Histogram<double> Beats = Meter.CreateHistogram<double>("beats");
                internal static void Emit(double a, double b) => Beats.Record(a < b ? b : a, LatticeTenantLabel.Platform);
            }
            """);

        var site = MetricEmissionScanner.Scan(_root)
            .Single(s => s.RelativePath == "src/Clamp.cs" && s.Instrument == "Beats");

        // Asserted against a literal, never against a value derived from the
        // same parse: a comparison of the parse with itself would hold whatever
        // the splitter did.
        Assert.That(
            site.Tags,
            Is.EqualTo("LatticeTenantLabel.Platform"),
            "A relational '<' in the measured value must not be read as a generic-argument "
            + "open; the tag arguments after it must still be parsed.");
    }

    [Test]
    public void A_generic_argument_list_containing_a_comma_is_not_mistaken_for_the_argument_separator()
    {
        // The converse case, and the reason the splitter tracks angle brackets
        // at all: the comma inside Dictionary<string, int> is not the boundary
        // between the measured value and the tags. A fix for the relational
        // case that simply stopped tracking '<' would split here instead.
        WriteSource("Generic.cs", """
            namespace Synthetic;
            internal static class Generic
            {
                internal static readonly Histogram<double> Beats = Meter.CreateHistogram<double>("beats");
                internal static void Emit(Dictionary<string, int> map) => Beats.Record(new Dictionary<string, int>(map).Count, LatticeTenantLabel.Platform);
            }
            """);

        var site = MetricEmissionScanner.Scan(_root)
            .Single(s => s.RelativePath == "src/Generic.cs" && s.Instrument == "Beats");

        Assert.That(
            site.Tags,
            Is.EqualTo("LatticeTenantLabel.Platform"),
            "A comma inside a generic argument list must not be read as the argument separator.");
    }

    [Test]
    public void A_greater_than_after_the_separator_does_not_make_the_comma_a_type_argument_comma()
    {
        // Deliberately synthetic, because it isolates one clause that nothing
        // else reaches. Both a real type-argument list and this expression are
        // balanced and enclose only type-shaped characters, so neither bracket
        // counting nor a character whitelist can tell them apart. Only the
        // token after the '>' can: here it is an identifier, so the '>' was the
        // greater-than operator and the comma before it really is the argument
        // separator. Without that check the splitter skips past the separator
        // and the tags read empty again, by a different route than the clamp.
        WriteSource("Straddle.cs", """
            namespace Synthetic;
            internal static class Straddle
            {
                internal static readonly Histogram<double> Beats = Meter.CreateHistogram<double>("beats");
                internal static void Emit(double a, double b, double c, double d) => Beats.Record(a < b, c > d);
            }
            """);

        var site = MetricEmissionScanner.Scan(_root)
            .Single(s => s.RelativePath == "src/Straddle.cs" && s.Instrument == "Beats");

        Assert.That(
            site.Tags,
            Is.EqualTo("c > d"),
            "A '<' balanced by a '>' that is followed by an identifier is a relational "
            + "operator, not a generic argument list, so the comma between them separates "
            + "the arguments.");
    }

    [Test]
    public void An_emission_with_no_tag_arguments_still_reports_an_empty_tag_list()
    {
        // The fail-closed direction, asserted on the observable the gate
        // actually consumes. Teaching the splitter to see through a relational
        // '<' must not turn an emission that genuinely passes no tags into one
        // that appears to pass them: empty tags are what the tenant-dimension
        // gate reports as missing, and that report must survive the change.
        WriteSource("Untagged.cs", """
            namespace Synthetic;
            internal static class Untagged
            {
                internal static readonly Histogram<double> Beats = Meter.CreateHistogram<double>("beats");
                internal static void Emit(double a, double b) => Beats.Record(a < b ? b : a);
            }
            """);

        var site = MetricEmissionScanner.Scan(_root)
            .Single(s => s.RelativePath == "src/Untagged.cs" && s.Instrument == "Beats");

        Assert.That(
            site.Tags,
            Is.EqualTo(string.Empty),
            "An emission that passes no tag arguments must still report an empty tag list, "
            + "so the tenant-dimension gate continues to report it as missing.");
    }
}
