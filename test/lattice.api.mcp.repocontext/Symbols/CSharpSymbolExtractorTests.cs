namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for <see cref="CSharpSymbolExtractor"/>, the Roslyn syntactic C#
/// declaration extractor. Parsing is purely syntactic (no compilation), so every
/// case here asserts what a single file yields in isolation: the fully-qualified
/// names, their <see cref="SymbolKind"/> classification, overload disambiguation by
/// parameter type, and graceful recovery from unparseable input.
/// </summary>
[TestFixture]
public sealed class CSharpSymbolExtractorTests
{
    private readonly CSharpSymbolExtractor _extractor = new();

    private static IReadOnlyDictionary<string, ExtractedSymbol> ByName(IReadOnlyList<ExtractedSymbol> symbols)
        => symbols.ToDictionary(s => s.FullyQualifiedName, StringComparer.Ordinal);

    [Test]
    public void Language_is_csharp()
        => Assert.That(_extractor.Language, Is.EqualTo("csharp"));

    [Test]
    public void Extracts_namespace_type_and_members_with_qualified_names()
    {
        const string source = """
            namespace Acme.Widgets;

            public class Gadget
            {
                public int Count { get; set; }
                private readonly string _name = "x";

                public void Run() { }
            }
            """;

        var symbols = ByName(_extractor.Extract("src/Gadget.cs", source));

        Assert.Multiple(() =>
        {
            Assert.That(symbols.ContainsKey("Acme.Widgets"), Is.True, "the namespace is captured");
            Assert.That(symbols["Acme.Widgets"].Kind, Is.EqualTo(SymbolKind.Namespace));
            Assert.That(symbols["Acme.Widgets.Gadget"].Kind, Is.EqualTo(SymbolKind.Type));
            Assert.That(symbols["Acme.Widgets.Gadget.Count"].Kind, Is.EqualTo(SymbolKind.Property));
            Assert.That(symbols["Acme.Widgets.Gadget._name"].Kind, Is.EqualTo(SymbolKind.Field));
            Assert.That(symbols.ContainsKey("Acme.Widgets.Gadget.Run()"), Is.True,
                "a method's fq name carries its parameter-type list");
            Assert.That(symbols["Acme.Widgets.Gadget.Run()"].Kind, Is.EqualTo(SymbolKind.Method));
        });
    }

    [Test]
    public void Disambiguates_method_overloads_by_parameter_types()
    {
        const string source = """
            namespace N;
            public class C
            {
                public void M(int a) { }
                public void M(string a, int b) { }
            }
            """;

        var names = _extractor.Extract("C.cs", source).Select(s => s.FullyQualifiedName).ToHashSet();

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("N.C.M(int)"));
            Assert.That(names, Does.Contain("N.C.M(string,int)"),
                "two overloads produce two distinct records rather than colliding on one key");
        });
    }

    [Test]
    public void Captures_interface_enum_and_enum_members()
    {
        const string source = """
            namespace N;
            public interface IShape { }
            public enum Color { Red, Green }
            """;

        var symbols = ByName(_extractor.Extract("N.cs", source));

        Assert.Multiple(() =>
        {
            Assert.That(symbols["N.IShape"].Kind, Is.EqualTo(SymbolKind.Interface));
            Assert.That(symbols["N.Color"].Kind, Is.EqualTo(SymbolKind.Enum));
            Assert.That(symbols["N.Color.Red"].Kind, Is.EqualTo(SymbolKind.Field));
            Assert.That(symbols["N.Color.Green"].Kind, Is.EqualTo(SymbolKind.Field));
        });
    }

    [Test]
    public void Captures_nested_types_with_dotted_names()
    {
        const string source = """
            namespace N;
            public class Outer
            {
                public class Inner
                {
                    public void Deep() { }
                }
            }
            """;

        var names = _extractor.Extract("N.cs", source).Select(s => s.FullyQualifiedName).ToHashSet();

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("N.Outer.Inner"));
            Assert.That(names, Does.Contain("N.Outer.Inner.Deep()"));
        });
    }

    [Test]
    public void Signature_is_a_single_collapsed_line()
    {
        const string source = """
            namespace N;
            public class C
            {
                public void M(
                    int a,
                    int b) { }
            }
            """;

        var method = _extractor.Extract("C.cs", source).Single(s => s.FullyQualifiedName == "N.C.M(int,int)");

        Assert.Multiple(() =>
        {
            Assert.That(method.Signature, Does.Not.Contain("\n"), "the display signature is a single line");
            Assert.That(method.Signature, Does.StartWith("public void M("));
            Assert.That(method.Signature, Does.Contain("int a, int b"), "the parameters collapse onto one line");
        });
    }

    [Test]
    public void Line_span_is_one_based()
    {
        // The type declaration starts on the second physical line (1-based).
        const string source = "namespace N;\npublic class C { }";

        var type = _extractor.Extract("C.cs", source).Single(s => s.FullyQualifiedName == "N.C");

        Assert.That(type.StartLine, Is.EqualTo(2));
    }

    [Test]
    public void Unparseable_input_recovers_what_it_can_without_throwing()
    {
        // A truncated type body: Roslyn still recovers the type declaration.
        const string source = "namespace N; public class C { public void M() {";

        IReadOnlyList<ExtractedSymbol> symbols = [];
        Assert.DoesNotThrow(() => symbols = _extractor.Extract("C.cs", source));
        Assert.That(symbols.Select(s => s.FullyQualifiedName), Does.Contain("N.C"));
    }

    [Test]
    public void Empty_source_yields_no_symbols()
        => Assert.That(_extractor.Extract("Empty.cs", string.Empty), Is.Empty);

    [Test]
    public void Null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => _extractor.Extract(null!, "x"));
            Assert.Throws<ArgumentNullException>(() => _extractor.Extract("x.cs", null!));
        });
    }
}
