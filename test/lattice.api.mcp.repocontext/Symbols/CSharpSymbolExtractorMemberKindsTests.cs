namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Additional unit tests for <see cref="CSharpSymbolExtractor"/> covering the
/// declaration kinds and type-reference forms not exercised by
/// <see cref="CSharpSymbolExtractorTests"/>: delegates, constructors, indexers,
/// events (property-style and field-style), record signatures, and the full set
/// of <c>TypeSyntax</c> shapes the reference collector decomposes (constraints,
/// operators, conversion operators, <c>typeof</c>/cast/default targets,
/// attributes, and qualified, alias-qualified, nullable, array, pointer, ref, and
/// tuple types).
/// </summary>
[TestFixture]
public sealed class CSharpSymbolExtractorMemberKindsTests
{
    private readonly CSharpSymbolExtractor _extractor = new();

    private static IReadOnlyDictionary<string, ExtractedSymbol> ByName(IReadOnlyList<ExtractedSymbol> symbols)
        => symbols.ToDictionary(s => s.FullyQualifiedName, StringComparer.Ordinal);

    [Test]
    public void Captures_a_namespace_level_delegate_as_an_other_symbol()
    {
        const string source = """
            namespace N;
            public delegate int Handler(string message);
            """;

        var symbols = ByName(_extractor.Extract("N.cs", source));

        Assert.Multiple(() =>
        {
            Assert.That(symbols.ContainsKey("N.Handler"), Is.True, "the delegate is captured");
            Assert.That(symbols["N.Handler"].Kind, Is.EqualTo(SymbolKind.Other));
            Assert.That(symbols["N.Handler"].Signature, Does.Contain("delegate int Handler"));
        });
    }

    [Test]
    public void Captures_constructor_indexer_and_events()
    {
        const string source = """
            namespace N;
            public class C
            {
                public C(int seed) { }
                public int this[int index] => index;
                public event System.EventHandler Changed { add { } remove { } }
                public event System.Action Ping;
            }
            """;

        var symbols = ByName(_extractor.Extract("C.cs", source));

        Assert.Multiple(() =>
        {
            Assert.That(symbols.ContainsKey("N.C.C(int)"), Is.True, "a constructor carries its parameter-type list");
            Assert.That(symbols["N.C.C(int)"].Kind, Is.EqualTo(SymbolKind.Method));
            Assert.That(symbols.ContainsKey("N.C.this(int)"), Is.True, "an indexer is keyed on 'this' plus its params");
            Assert.That(symbols["N.C.this(int)"].Kind, Is.EqualTo(SymbolKind.Property));
            Assert.That(symbols.ContainsKey("N.C.Changed"), Is.True, "a property-style event is captured");
            Assert.That(symbols["N.C.Changed"].Kind, Is.EqualTo(SymbolKind.Field), "a property-style event");
            Assert.That(symbols.ContainsKey("N.C.Ping"), Is.True, "a field-style event is captured");
            Assert.That(symbols["N.C.Ping"].Kind, Is.EqualTo(SymbolKind.Field), "a field-style event");
        });
    }

    [Test]
    public void Captures_a_field_style_event_with_multiple_declarators()
    {
        const string source = """
            namespace N;
            public class C
            {
                public event System.Action First, Second;
            }
            """;

        var names = _extractor.Extract("C.cs", source).Select(s => s.FullyQualifiedName).ToHashSet();

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("N.C.First"));
            Assert.That(names, Does.Contain("N.C.Second"), "each declarator in one event field yields its own record");
        });
    }

    [Test]
    public void Record_struct_signature_names_the_record_kind_keyword()
    {
        const string source = """
            namespace N;
            public record struct Money(int Cents);
            """;

        var record = _extractor.Extract("Money.cs", source).Single(s => s.FullyQualifiedName == "N.Money");

        Assert.That(record.Signature, Does.Contain("record struct Money"),
            "a record's class-or-struct keyword follows the 'record' keyword in the signature");
    }

    private static IReadOnlyList<string> ReferencesOf(IReadOnlyList<ExtractedSymbol> symbols, string fqName)
        => symbols.Single(s => s.FullyQualifiedName == fqName).ReferencedNames;

    [Test]
    public void References_capture_a_generic_constraint_type()
    {
        const string source = """
            namespace N;
            public class C<T> where T : Base
            {
                public void M(T item) { }
            }
            """;

        Assert.That(ReferencesOf(_extractor.Extract("C.cs", source), "N.C<T>"), Does.Contain("Base"),
            "a type-constraint on a type parameter is a reference");
    }

    [Test]
    public void References_capture_operator_and_conversion_operator_return_types()
    {
        const string source = """
            namespace N;
            public class C
            {
                public static Money operator +(C left, C right) => default;
                public static explicit operator Dollars(C value) => default;
            }
            """;

        var references = ReferencesOf(_extractor.Extract("C.cs", source), "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(references, Does.Contain("Money"), "an operator's return type is a reference");
            Assert.That(references, Does.Contain("Dollars"), "a conversion operator's target type is a reference");
        });
    }

    [Test]
    public void References_capture_typeof_cast_and_default_targets()
    {
        const string source = """
            namespace N;
            public class C
            {
                public void M()
                {
                    var t = typeof(Widget);
                    var g = (Gadget)t;
                    var d = default(Doohickey);
                }
            }
            """;

        var references = ReferencesOf(_extractor.Extract("C.cs", source), "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(references, Does.Contain("Widget"), "a typeof target is a reference");
            Assert.That(references, Does.Contain("Gadget"), "a cast target is a reference");
            Assert.That(references, Does.Contain("Doohickey"), "a default(T) target is a reference");
        });
    }

    [Test]
    public void References_capture_an_attribute_name()
    {
        const string source = """
            namespace N;
            public class C
            {
                [Audited]
                public void M() { }
            }
            """;

        Assert.That(ReferencesOf(_extractor.Extract("C.cs", source), "N.C"), Does.Contain("Audited"),
            "an attribute application is a reference");
    }

    [Test]
    public void References_decompose_qualified_alias_nullable_array_and_pointer_types()
    {
        const string source = """
            namespace N;
            public unsafe class C
            {
                public System.Text.Builder Qualified { get; set; }
                public global::Widget Aliased { get; set; }
                public Payload? Nullable { get; set; }
                public Request[] Array { get; set; }
                public Gizmo* Pointer { get; set; }
            }
            """;

        var references = ReferencesOf(_extractor.Extract("C.cs", source), "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(references, Does.Contain("Builder"), "a qualified name contributes only its right-most segment");
            Assert.That(references, Does.Contain("Widget"), "an alias-qualified name contributes its named type");
            Assert.That(references, Does.Contain("Payload"), "a nullable type contributes its element type");
            Assert.That(references, Does.Contain("Request"), "an array type contributes its element type");
            Assert.That(references, Does.Contain("Gizmo"), "a pointer type contributes its element type");
        });
    }

    [Test]
    public void References_decompose_ref_return_and_tuple_types()
    {
        const string source = """
            namespace N;
            public class C
            {
                private Thingy _thing;
                public ref Thingy GetRef() => ref _thing;
                public (Alpha First, Beta Second) Pair() => default;
            }
            """;

        var references = ReferencesOf(_extractor.Extract("C.cs", source), "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(references, Does.Contain("Thingy"), "a ref return type contributes its element type");
            Assert.That(references, Does.Contain("Alpha"), "a tuple element type is a reference");
            Assert.That(references, Does.Contain("Beta"), "each tuple element type is a reference");
        });
    }
}
