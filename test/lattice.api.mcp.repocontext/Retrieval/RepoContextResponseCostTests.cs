using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextResponseCost"/>, the model that makes the
/// context bundle's budget bound the response a caller actually receives rather than
/// only the source text inside it (issue #1811).
/// <para>
/// The dual-emission constant is the load-bearing number in that model, so it is not
/// asserted against itself: it is re-measured here against real serialization, at the
/// worst case (slices detail over multi-line C# source, which escapes worst), and the
/// test fails if the constant ever stops bounding the measured ratio.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextResponseCostTests
{
    private static readonly IRepoContextTokenCounter Counter =
        new TiktokenRepoContextTokenCounter(new RepoContextIndexingOptions());

    /// <summary>
    /// A representative worst case: real multi-line C# source, which carries the quotes,
    /// backslashes and newlines that inflate the escaped text block relative to the
    /// structured block.
    /// </summary>
    private const string SourceBody = """
        using System.Text;

        namespace Acme.Widgets;

        /// <summary>Formats a "widget" label, escaping \ and " as needed.</summary>
        public sealed class WidgetFormatter
        {
            private readonly StringBuilder _builder = new();

            public string Format(string name, int count)
            {
                _builder.Clear();
                _builder.Append('"').Append(name).Append('"');
                _builder.Append(" x ").Append(count);
                return _builder.ToString();
            }
        }
        """;

    /// <summary>
    /// Measures the real dual-emission ratio: the MCP SDK emits every tool result twice -
    /// once as a structured block and once as a text block, the latter being the same JSON
    /// escaped inside a JSON string - so the pair costs strictly more than 2x one copy.
    /// <para>
    /// Run across payload shapes because the worst case is <b>not</b> the intuitive one.
    /// A large source body measures around 2.8, but a small paths bundle measures around
    /// 3.4: escaping inflates quotes, JSON scaffolding is quote-dense, and on a small
    /// payload that scaffolding dominates instead of being diluted by content. A constant
    /// tuned only on source payloads would therefore under-state small ones.
    /// </para>
    /// </summary>
    [TestCase(PayloadShape.SourceSlices)]
    [TestCase(PayloadShape.PathsOnly)]
    [TestCase(PayloadShape.OutlineSymbols)]
    public void DualEmission_constant_bounds_the_measured_ratio(PayloadShape shape)
    {
        var result = BundleWith(BodyFor(shape));
        var structured = JsonSerializer.Serialize(result, LatticeApiMcpToolSerialization.Options);

        // The text block is the structured JSON escaped into a JSON string value - exactly
        // what serializing the JSON *as a string* produces.
        var textBlock = JsonSerializer.Serialize(structured, LatticeApiMcpToolSerialization.Options);

        var singleCopy = Counter.CountTokens(structured);
        var pair = singleCopy + Counter.CountTokens(textBlock);
        var measured = (double)pair / singleCopy;
        var constant = (double)RepoContextResponseCost.DualEmissionNumerator
            / RepoContextResponseCost.DualEmissionDenominator;

        Assert.Multiple(() =>
        {
            Assert.That(measured, Is.GreaterThan(2.0),
                "Escaping the text block makes it strictly larger than the structured block, so the pair always exceeds 2x.");
            Assert.That(constant, Is.GreaterThanOrEqualTo(measured),
                $"The dual-emission constant ({constant:F2}) must bound the measured ratio ({measured:F2}) or the "
                + "budget under-states the response and the ceiling can be exceeded - the defect this model fixes. "
                + "Raise RepoContextResponseCost.DualEmissionNumerator.");
        });
    }

    /// <summary>The payload shapes the dual-emission ratio is measured across.</summary>
    public enum PayloadShape
    {
        /// <summary>Multi-line C# source at slices detail: quote- and newline-dense, the worst case.</summary>
        SourceSlices,

        /// <summary>A bare repository-relative path, as a paths-detail entry carries.</summary>
        PathsOnly,

        /// <summary>A newline-joined declared-symbol skeleton, as an outline-detail entry carries.</summary>
        OutlineSymbols,
    }

    private static string BodyFor(PayloadShape shape) => shape switch
    {
        PayloadShape.PathsOnly => "src/lattice/Widgets/WidgetFormatter.cs",
        PayloadShape.OutlineSymbols =>
            "public sealed class WidgetFormatter\npublic string Format(string name, int count)\npublic void Reset()",
        _ => SourceBody,
    };

    [Test]
    public void WithDualEmission_rounds_up_so_the_estimate_never_understates()
    {
        // 10 * 35 / 10 = 35 exactly; 1 * 35 / 10 = 3.5 -> must round up to 4, never down to 3.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextResponseCost.WithDualEmission(10), Is.EqualTo(35));
            Assert.That(RepoContextResponseCost.WithDualEmission(1), Is.EqualTo(4),
                "The estimate must round against the caller's budget, never in favour of it.");
        });
    }

    [Test]
    public void WithDualEmission_non_positive_costs_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextResponseCost.WithDualEmission(0), Is.Zero);
            Assert.That(RepoContextResponseCost.WithDualEmission(-5), Is.Zero);
        });
    }

    [Test]
    public void EntryEnvelopeTokens_charges_more_than_the_content_alone()
    {
        var units = new[]
        {
            new RepoContextRenderedUnit("receipt-abc", "span", null, 4, "body"),
        };

        var envelope = RepoContextResponseCost.EntryEnvelopeTokens(
            contentTokens: 4,
            path: "src/lattice/Some/Deeply/Nested/WidgetFormatter.cs",
            reasons: ["semantic", "chunk:file"],
            contentHash: new string('a', 64),
            units: units,
            counter: Counter);

        Assert.That(envelope, Is.GreaterThan(4),
            "An entry ships its path, reasons, hash and unit receipts alongside its content; charging content alone is the bug.");
    }

    [Test]
    public void EntryEnvelopeTokens_null_arguments_throw()
    {
        var units = Array.Empty<RepoContextRenderedUnit>();
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextResponseCost.EntryEnvelopeTokens(1, null!, [], null, units, Counter),
                Throws.ArgumentNullException);
            Assert.That(() => RepoContextResponseCost.EntryEnvelopeTokens(1, "p", null!, null, units, Counter),
                Throws.ArgumentNullException);
            Assert.That(() => RepoContextResponseCost.EntryEnvelopeTokens(1, "p", [], null, null!, Counter),
                Throws.ArgumentNullException);
            Assert.That(() => RepoContextResponseCost.EntryEnvelopeTokens(1, "p", [], null, units, null!),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// The regression that names the original defect: the delivered source text must
    /// appear exactly once in the serialized bundle. Before the fix an entry carried its
    /// body in both <c>content</c> and every <c>units[].content</c>, so each byte of
    /// source crossed the wire twice per copy, and four times across the emitted pair.
    /// </summary>
    [Test]
    public void Serialized_bundle_carries_the_delivered_source_exactly_once()
    {
        var result = BundleWith(SourceBody);
        var json = JsonSerializer.Serialize(result, LatticeApiMcpToolSerialization.Options);

        // Compare on a distinctive fragment of the body, serialized the same way, so the
        // needle is escaped exactly as it appears inside the payload.
        var needle = JsonSerializer.Serialize("_builder.Append(\" x \").Append(count);").Trim('"');
        var occurrences = CountOccurrences(json, needle);

        Assert.That(occurrences, Is.EqualTo(1),
            "The delivered body must ship once, on the entry. A unit is a descriptor, not a second copy (#1811).");
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (index >= 0)
        {
            count++;
            index = haystack.IndexOf(needle, index + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }

    private static RepoContextContextResult BundleWith(string body)
    {
        var candidate = new RepoContextBundlePacker.Candidate(
            "src/lattice/Widgets/WidgetFormatter.cs",
            0.91,
            ["semantic", "chunk:file"],
            body,
            420,
            new string('c', 64),
            [new RepoContextRenderedUnit("receipt-1", "span", null, Counter.CountTokens(body), body)]);

        var packed = RepoContextBundlePacker.Pack([candidate], 100_000, Counter);

        return new RepoContextContextResult
        {
            RepoId = "lattice",
            Task = "format a widget label",
            Mode = "semantic",
            RetrievalPath = RepoContextRetrievalPath.SemanticExact,
            Detail = "slices",
            BudgetTokens = 100_000,
            TotalTokens = packed.TotalTokens,
            ResponseTokens = packed.ResponseTokens,
            Truncated = packed.Truncated,
            RetryBudgetTokens = null,
            Entries = packed.Entries,
            Session = "measurement",
        };
    }
}
