using System.Text;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Allocation guard for the in-brace label scan of
/// <see cref="PromQlMetricExtractor.ExtractReferences(string)"/>: only the reserved
/// <c>__name__</c> label names a metric, so every other label name must be matched by
/// span comparison and never materialised as a substring.
/// </summary>
public sealed partial class PromQlMetricExtractorTests
{
    [Test]
    public void ExtractReferences_allocates_nothing_per_ordinary_in_brace_label()
    {
        // Regression guard for #6195: replacing the span comparison with
        // query.Substring(...) == "__name__" keeps every behavioural test green but
        // allocates one string per label, which this differential exposes.
        var growth = AllocationProbe.Growth(
            prepare: static size => Selector(size),
            measure: static (query, _) =>
            {
                var references = PromQlMetricExtractor.ExtractReferences(query);
                AllocationProbe.ScalarSink += references.Names.Count;
            },
            smallSize: 10,
            largeSize: 2_000);

        Assert.That(growth, Is.Zero, "Each non-__name__ label inside a selector must allocate nothing.");
    }

    [Test]
    public void The_allocation_selector_has_the_expected_shape()
    {
        // Non-vacuous: the probed query really is an anchored selector carrying many
        // ordinary labels and no __name__ matcher, so it exercises the in-brace arm.
        var query = Selector(3);
        var references = PromQlMetricExtractor.ExtractReferences(query);

        Assert.Multiple(() =>
        {
            Assert.That(query, Is.EqualTo("up{label0=\"v\",label1=\"v\",label2=\"v\"}"));
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
        });
    }

    private static string Selector(int labels)
    {
        var builder = new StringBuilder("up{");
        for (var i = 0; i < labels; i++)
        {
            if (i > 0)
            {
                builder.Append(',');
            }

            builder.Append("label").Append(i).Append("=\"v\"");
        }

        return builder.Append('}').ToString();
    }
}
