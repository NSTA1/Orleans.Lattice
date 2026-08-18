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

    private static IReadOnlyList<string> ReferencesOf(IReadOnlyList<ExtractedSymbol> symbols, string fqName)
        => symbols.Single(s => s.FullyQualifiedName == fqName).ReferencedNames;

    [Test]
    public void Type_with_no_references_has_an_empty_reference_set()
    {
        const string source = "namespace N; public class C { public void M() { } }";

        Assert.That(ReferencesOf(_extractor.Extract("C.cs", source), "N.C"), Is.Empty);
    }

    [Test]
    public void References_capture_base_types_members_and_generic_arguments()
    {
        const string source = """
            namespace N;
            public class Widget : Base, IThing
            {
                private Helper _helper;
                public Gadget Gadget { get; set; }
                public List<Payload> Load(Request request) => new List<Payload>();
                public void Make() { var made = new Doohickey(); }
            }
            """;

        var references = ReferencesOf(_extractor.Extract("Widget.cs", source), "N.Widget");

        Assert.Multiple(() =>
        {
            Assert.That(references, Does.Contain("Base"), "a base class is a reference");
            Assert.That(references, Does.Contain("IThing"), "an implemented interface is a reference");
            Assert.That(references, Does.Contain("Helper"), "a field type is a reference");
            Assert.That(references, Does.Contain("Gadget"), "a property type is a reference");
            Assert.That(references, Does.Contain("Request"), "a parameter type is a reference");
            Assert.That(references, Does.Contain("List"), "a generic return type is a reference");
            Assert.That(references, Does.Contain("Payload"), "a generic argument is a reference");
            Assert.That(references, Does.Contain("Doohickey"), "an object-creation type is a reference");
        });
    }

    [Test]
    public void References_are_sorted_and_deduplicated()
    {
        const string source = """
            namespace N;
            public class C
            {
                public Alpha A { get; set; }
                public Alpha B { get; set; }
                public Zeta Z { get; set; }
            }
            """;

        Assert.That(ReferencesOf(_extractor.Extract("C.cs", source), "N.C"),
            Is.EqualTo(new[] { "Alpha", "Zeta" }), "duplicates collapse and the set is ordinal-sorted");
    }

    [Test]
    public void References_exclude_the_declaring_type_its_type_parameters_predefined_types_and_var()
    {
        const string source = """
            namespace N;
            public class Box<T>
            {
                public Box<T> Self { get; set; }
                public T Item { get; set; }
                public int Count { get; set; }
                public void Fill() { var x = 1; }
            }
            """;

        var references = ReferencesOf(_extractor.Extract("Box.cs", source), "N.Box<T>");

        Assert.That(references, Is.Empty,
            "the type refers only to itself, its own type parameter, a predefined type, and var - none are edges");
    }
}
