using System.IO;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Unit tests for <see cref="MetricEmissionScanner"/>'s instrument-scoping
/// rules. The scanner discovers instrument names per declaring type (unioned
/// across a type's partial-class files) rather than globally, so a member that
/// merely shares a name with an instrument declared in an unrelated type is not
/// mistaken for an emission - while a bare emission in a partial of the
/// declaring type is still caught.
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
}
