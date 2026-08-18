namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Graph;

/// <summary>
/// Unit tests for the graph-tool result payloads (<see cref="RepoContextOutlineResult"/>,
/// <see cref="RepoContextOutlineSymbol"/>, <see cref="RepoContextChangedResult"/>,
/// <see cref="RepoContextRelatedResult"/>, and <see cref="RepoContextRelatedEdge"/>).
/// They are plain records projected to JSON by the MCP SDK, so the coverage pins that
/// every member round-trips the value it was constructed with.
/// </summary>
[TestFixture]
public sealed class RepoContextGraphResultTests
{
    [Test]
    public void Outline_symbol_carries_every_member()
    {
        var symbol = new RepoContextOutlineSymbol
        {
            FullyQualifiedName = "N.Widget",
            Kind = "Type",
            Signature = "public class Widget",
            StartLine = 3,
            EndLine = 9,
        };

        Assert.Multiple(() =>
        {
            Assert.That(symbol.FullyQualifiedName, Is.EqualTo("N.Widget"));
            Assert.That(symbol.Kind, Is.EqualTo("Type"));
            Assert.That(symbol.Signature, Is.EqualTo("public class Widget"));
            Assert.That(symbol.StartLine, Is.EqualTo(3));
            Assert.That(symbol.EndLine, Is.EqualTo(9));
        });
    }

    [Test]
    public void Outline_result_carries_every_member()
    {
        var symbols = new[]
        {
            new RepoContextOutlineSymbol
            {
                FullyQualifiedName = "N.Widget", Kind = "Type", Signature = "class Widget",
                StartLine = 1, EndLine = 2,
            },
        };
        var result = new RepoContextOutlineResult
        {
            RepoId = "acme",
            Path = "src/Widget.cs",
            Exists = true,
            FullReadTokenCount = 42,
            Symbols = symbols,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Path, Is.EqualTo("src/Widget.cs"));
            Assert.That(result.Exists, Is.True);
            Assert.That(result.FullReadTokenCount, Is.EqualTo(42));
            Assert.That(result.Symbols, Is.SameAs(symbols));
        });
    }

    [Test]
    public void Outline_result_allows_a_null_token_count_for_an_unprocessed_file()
    {
        var result = new RepoContextOutlineResult
        {
            RepoId = "acme",
            Path = "src/Widget.cs",
            Exists = false,
            FullReadTokenCount = null,
            Symbols = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.False);
            Assert.That(result.FullReadTokenCount, Is.Null);
            Assert.That(result.Symbols, Is.Empty);
        });
    }

    [Test]
    public void Changed_result_carries_every_member()
    {
        var result = new RepoContextChangedResult
        {
            RepoId = "acme",
            Added = ["a.cs"],
            Updated = ["b.cs"],
            Removed = ["c.cs"],
            Dependents = ["d.cs"],
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Added, Is.EquivalentTo(new[] { "a.cs" }));
            Assert.That(result.Updated, Is.EquivalentTo(new[] { "b.cs" }));
            Assert.That(result.Removed, Is.EquivalentTo(new[] { "c.cs" }));
            Assert.That(result.Dependents, Is.EquivalentTo(new[] { "d.cs" }));
        });
    }

    [Test]
    public void Related_edge_carries_symbol_and_optional_path()
    {
        var resolved = new RepoContextRelatedEdge { Symbol = "N.A", Path = "src/A.cs" };
        var unresolved = new RepoContextRelatedEdge { Symbol = "N.B", Path = null };

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Symbol, Is.EqualTo("N.A"));
            Assert.That(resolved.Path, Is.EqualTo("src/A.cs"));
            Assert.That(unresolved.Symbol, Is.EqualTo("N.B"));
            Assert.That(unresolved.Path, Is.Null);
        });
    }

    [Test]
    public void Related_result_carries_every_member()
    {
        var dependents = new[] { new RepoContextRelatedEdge { Symbol = "N.A", Path = "src/A.cs" } };
        var tests = new[] { new RepoContextRelatedEdge { Symbol = "N.WidgetTests", Path = "test/WidgetTests.cs" } };
        var result = new RepoContextRelatedResult
        {
            RepoId = "acme",
            Path = "src/Widget.cs",
            Exists = true,
            Imports = ["List", "Task"],
            Dependents = dependents,
            Tests = tests,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Path, Is.EqualTo("src/Widget.cs"));
            Assert.That(result.Exists, Is.True);
            Assert.That(result.Imports, Is.EquivalentTo(new[] { "List", "Task" }));
            Assert.That(result.Dependents, Is.SameAs(dependents));
            Assert.That(result.Tests, Is.SameAs(tests));
        });
    }
}
