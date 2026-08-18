using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Extracts C# declarations - namespaces, types, and their members - from source
/// text using the Roslyn syntax parser. Parsing is purely syntactic (no compilation,
/// no semantic model, no project or reference resolution), so a single file is
/// analysed in isolation cheaply and a file that fails to parse yields whatever
/// declarations Roslyn still recovered rather than an error.
/// <para>
/// A member's fully-qualified name is disambiguated by its parameter-type list for
/// overloadable members (methods, constructors, indexers), so two overloads produce
/// two distinct symbol records rather than colliding on one key.
/// </para>
/// </summary>
internal sealed class CSharpSymbolExtractor : ILanguageSymbolExtractor
{
    private static readonly CSharpParseOptions ParseOptions =
        new(LanguageVersion.Preview, DocumentationMode.None, SourceCodeKind.Regular);

    /// <inheritdoc />
    public string Language => "csharp";

    /// <inheritdoc />
    public IReadOnlyList<ExtractedSymbol> Extract(string relativePath, string content)
    {
        ArgumentNullException.ThrowIfNull(relativePath);
        ArgumentNullException.ThrowIfNull(content);

        var tree = CSharpSyntaxTree.ParseText(content, ParseOptions);
        if (tree.GetRoot() is not CompilationUnitSyntax root)
        {
            return [];
        }

        var symbols = new List<ExtractedSymbol>();
        WalkMembers(root.Members, prefix: string.Empty, symbols);
        return symbols;
    }

    private static void WalkMembers(
        SyntaxList<MemberDeclarationSyntax> members, string prefix, List<ExtractedSymbol> symbols)
    {
        foreach (var member in members)
        {
            WalkMember(member, prefix, symbols);
        }
    }

    private static void WalkMember(
        MemberDeclarationSyntax member, string prefix, List<ExtractedSymbol> symbols)
    {
        switch (member)
        {
            case BaseNamespaceDeclarationSyntax ns:
            {
                var name = ns.Name.ToString();
                var fqName = Combine(prefix, name);
                symbols.Add(Build(fqName, SymbolKind.Namespace, ns, $"namespace {fqName}"));
                WalkMembers(ns.Members, fqName, symbols);
                break;
            }

            case TypeDeclarationSyntax type:
            {
                var fqName = Combine(prefix, type.Identifier.Text + TypeParameters(type.TypeParameterList));
                var kind = type is InterfaceDeclarationSyntax ? SymbolKind.Interface : SymbolKind.Type;
                symbols.Add(Build(fqName, kind, type, TypeSignature(type), CollectTypeReferences(type)));
                WalkMembers(type.Members, fqName, symbols);
                break;
            }

            case EnumDeclarationSyntax enumDecl:
            {
                var fqName = Combine(prefix, enumDecl.Identifier.Text);
                symbols.Add(Build(fqName, SymbolKind.Enum, enumDecl,
                    $"{Modifiers(enumDecl.Modifiers)}enum {enumDecl.Identifier.Text}".Trim()));
                foreach (var value in enumDecl.Members)
                {
                    var valueFq = Combine(fqName, value.Identifier.Text);
                    symbols.Add(Build(valueFq, SymbolKind.Field, value, value.Identifier.Text));
                }

                break;
            }

            case DelegateDeclarationSyntax del:
            {
                var fqName = Combine(prefix, del.Identifier.Text + TypeParameters(del.TypeParameterList));
                var signature =
                    $"{Modifiers(del.Modifiers)}delegate {del.ReturnType} {del.Identifier.Text}{Parameters(del.ParameterList)}";
                symbols.Add(Build(fqName, SymbolKind.Other, del, Collapse(signature)));
                break;
            }

            case MethodDeclarationSyntax method:
            {
                var name = method.Identifier.Text + TypeParameters(method.TypeParameterList);
                var fqName = Combine(prefix, name) + ParameterTypes(method.ParameterList);
                var signature =
                    $"{Modifiers(method.Modifiers)}{method.ReturnType} {name}{Parameters(method.ParameterList)}";
                symbols.Add(Build(fqName, SymbolKind.Method, method, Collapse(signature)));
                break;
            }

            case ConstructorDeclarationSyntax ctor:
            {
                var fqName = Combine(prefix, ctor.Identifier.Text) + ParameterTypes(ctor.ParameterList);
                var signature = $"{Modifiers(ctor.Modifiers)}{ctor.Identifier.Text}{Parameters(ctor.ParameterList)}";
                symbols.Add(Build(fqName, SymbolKind.Method, ctor, Collapse(signature)));
                break;
            }

            case PropertyDeclarationSyntax property:
            {
                var fqName = Combine(prefix, property.Identifier.Text);
                var signature = $"{Modifiers(property.Modifiers)}{property.Type} {property.Identifier.Text}";
                symbols.Add(Build(fqName, SymbolKind.Property, property, Collapse(signature)));
                break;
            }

            case IndexerDeclarationSyntax indexer:
            {
                var fqName = Combine(prefix, "this") + ParameterTypes(indexer.ParameterList);
                var signature = $"{Modifiers(indexer.Modifiers)}{indexer.Type} this{Parameters(indexer.ParameterList)}";
                symbols.Add(Build(fqName, SymbolKind.Property, indexer, Collapse(signature)));
                break;
            }

            case EventDeclarationSyntax evt:
            {
                var fqName = Combine(prefix, evt.Identifier.Text);
                var signature = $"{Modifiers(evt.Modifiers)}event {evt.Type} {evt.Identifier.Text}";
                symbols.Add(Build(fqName, SymbolKind.Field, evt, Collapse(signature)));
                break;
            }

            case EventFieldDeclarationSyntax evtField:
            {
                foreach (var variable in evtField.Declaration.Variables)
                {
                    var fqName = Combine(prefix, variable.Identifier.Text);
                    var signature =
                        $"{Modifiers(evtField.Modifiers)}event {evtField.Declaration.Type} {variable.Identifier.Text}";
                    symbols.Add(Build(fqName, SymbolKind.Field, variable, Collapse(signature)));
                }

                break;
            }

            case FieldDeclarationSyntax field:
            {
                foreach (var variable in field.Declaration.Variables)
                {
                    var fqName = Combine(prefix, variable.Identifier.Text);
                    var signature =
                        $"{Modifiers(field.Modifiers)}{field.Declaration.Type} {variable.Identifier.Text}";
                    symbols.Add(Build(fqName, SymbolKind.Field, variable, Collapse(signature)));
                }

                break;
            }
        }
    }

    private static ExtractedSymbol Build(
        string fqName, SymbolKind kind, SyntaxNode node, string signature, IReadOnlyList<string>? references = null)
    {
        var span = node.GetLocation().GetLineSpan();
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes(node.ToString()));
        return new ExtractedSymbol(
            fqName,
            kind,
            span.StartLinePosition.Line + 1,
            span.EndLinePosition.Line + 1,
            signature,
            digest)
        {
            ReferencedNames = references ?? [],
        };
    }

    /// <summary>
    /// Collects the distinct simple type-names a type declaration references
    /// syntactically: its base types, generic constraints, the types named in its
    /// members' signatures (parameter, return, field, property, event, and indexer
    /// types), and the types named in member bodies (object creations, <c>typeof</c>
    /// and cast/default targets, and local declarations). The walk descends the whole
    /// type but stops at a nested type declaration so a nested type's references are
    /// attributed to that nested type's own record, not the outer one. The type's own
    /// name, its type-parameter names, and predefined language types (<c>int</c>,
    /// <c>string</c>, ...) are excluded. Names are ordinal-sorted and de-duplicated so
    /// an unchanged type re-extracts to the same list.
    /// </summary>
    private static IReadOnlyList<string> CollectTypeReferences(TypeDeclarationSyntax type)
    {
        var exclude = new HashSet<string>(StringComparer.Ordinal) { type.Identifier.Text };
        if (type.TypeParameterList is { } typeParameters)
        {
            foreach (var parameter in typeParameters.Parameters)
            {
                exclude.Add(parameter.Identifier.Text);
            }
        }

        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var node in type.DescendantNodes(
            descendIntoChildren: n => ReferenceEquals(n, type) || n is not BaseTypeDeclarationSyntax))
        {
            switch (node)
            {
                case BaseTypeSyntax baseType:
                    AddTypeNames(baseType.Type, names, exclude);
                    break;
                case TypeParameterConstraintClauseSyntax constraintClause:
                    foreach (var constraint in constraintClause.Constraints)
                    {
                        if (constraint is TypeConstraintSyntax typeConstraint)
                        {
                            AddTypeNames(typeConstraint.Type, names, exclude);
                        }
                    }

                    break;
                case ParameterSyntax parameter:
                    AddTypeNames(parameter.Type, names, exclude);
                    break;
                case MethodDeclarationSyntax method:
                    AddTypeNames(method.ReturnType, names, exclude);
                    break;
                case OperatorDeclarationSyntax op:
                    AddTypeNames(op.ReturnType, names, exclude);
                    break;
                case ConversionOperatorDeclarationSyntax conversion:
                    AddTypeNames(conversion.Type, names, exclude);
                    break;
                case PropertyDeclarationSyntax property:
                    AddTypeNames(property.Type, names, exclude);
                    break;
                case IndexerDeclarationSyntax indexer:
                    AddTypeNames(indexer.Type, names, exclude);
                    break;
                case EventDeclarationSyntax evt:
                    AddTypeNames(evt.Type, names, exclude);
                    break;
                case DelegateDeclarationSyntax del:
                    AddTypeNames(del.ReturnType, names, exclude);
                    break;
                case VariableDeclarationSyntax variable:
                    AddTypeNames(variable.Type, names, exclude);
                    break;
                case ObjectCreationExpressionSyntax creation:
                    AddTypeNames(creation.Type, names, exclude);
                    break;
                case TypeOfExpressionSyntax typeOf:
                    AddTypeNames(typeOf.Type, names, exclude);
                    break;
                case CastExpressionSyntax cast:
                    AddTypeNames(cast.Type, names, exclude);
                    break;
                case DefaultExpressionSyntax defaultExpression:
                    AddTypeNames(defaultExpression.Type, names, exclude);
                    break;
                case AttributeSyntax attribute:
                    AddTypeNames(attribute.Name, names, exclude);
                    break;
            }
        }

        return names.Count == 0 ? [] : [.. names];
    }

    /// <summary>
    /// Decomposes a <see cref="TypeSyntax"/> into the simple identifiers it names and
    /// adds each (unless excluded) to <paramref name="names"/>. Generic arguments,
    /// array/nullable/pointer element types, tuple element types, and the right side
    /// of a qualified name are followed so a name like
    /// <c>System.Collections.Generic.List&lt;Foo&gt;</c> contributes <c>List</c> and
    /// <c>Foo</c> but no namespace segment. Predefined types and the <c>var</c>
    /// contextual keyword are ignored.
    /// </summary>
    private static void AddTypeNames(TypeSyntax? type, SortedSet<string> names, HashSet<string> exclude)
    {
        switch (type)
        {
            case null or PredefinedTypeSyntax:
                return;
            case IdentifierNameSyntax identifier:
            {
                var text = identifier.Identifier.Text;
                if (text.Length != 0 && text != "var" && !exclude.Contains(text))
                {
                    names.Add(text);
                }

                break;
            }

            case GenericNameSyntax generic:
            {
                var text = generic.Identifier.Text;
                if (text.Length != 0 && !exclude.Contains(text))
                {
                    names.Add(text);
                }

                foreach (var argument in generic.TypeArgumentList.Arguments)
                {
                    AddTypeNames(argument, names, exclude);
                }

                break;
            }

            case QualifiedNameSyntax qualified:
                AddTypeNames(qualified.Right, names, exclude);
                break;
            case AliasQualifiedNameSyntax aliasQualified:
                AddTypeNames(aliasQualified.Name, names, exclude);
                break;
            case NullableTypeSyntax nullable:
                AddTypeNames(nullable.ElementType, names, exclude);
                break;
            case ArrayTypeSyntax array:
                AddTypeNames(array.ElementType, names, exclude);
                break;
            case PointerTypeSyntax pointer:
                AddTypeNames(pointer.ElementType, names, exclude);
                break;
            case RefTypeSyntax refType:
                AddTypeNames(refType.Type, names, exclude);
                break;
            case TupleTypeSyntax tuple:
                foreach (var element in tuple.Elements)
                {
                    AddTypeNames(element.Type, names, exclude);
                }

                break;
        }
    }

    private static string Combine(string prefix, string name) =>
        prefix.Length == 0 ? name : $"{prefix}.{name}";

    private static string TypeSignature(TypeDeclarationSyntax type)
    {
        var keyword = type.Keyword.Text;
        // A record's kind keyword ("class"/"struct") follows the "record" keyword.
        if (type is RecordDeclarationSyntax { ClassOrStructKeyword.Text.Length: > 0 } record)
        {
            keyword = $"record {record.ClassOrStructKeyword.Text}";
        }

        return Collapse(
            $"{Modifiers(type.Modifiers)}{keyword} {type.Identifier.Text}{TypeParameters(type.TypeParameterList)}");
    }

    private static string Modifiers(SyntaxTokenList modifiers)
    {
        if (modifiers.Count == 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        foreach (var modifier in modifiers)
        {
            builder.Append(modifier.Text).Append(' ');
        }

        return builder.ToString();
    }

    private static string TypeParameters(TypeParameterListSyntax? list) =>
        list is null ? string.Empty : Collapse(list.ToString());

    private static string Parameters(BaseParameterListSyntax? list) =>
        list is null ? "()" : Collapse(list.ToString());

    private static string ParameterTypes(BaseParameterListSyntax? list)
    {
        if (list is null || list.Parameters.Count == 0)
        {
            return "()";
        }

        var builder = new StringBuilder("(");
        var first = true;
        foreach (var parameter in list.Parameters)
        {
            if (!first)
            {
                builder.Append(',');
            }

            first = false;
            builder.Append(parameter.Type is { } type ? Collapse(type.ToString()) : "?");
        }

        return builder.Append(')').ToString();
    }

    private static string Collapse(string text)
    {
        var builder = new StringBuilder(text.Length);
        var lastWasSpace = false;
        foreach (var ch in text)
        {
            if (char.IsWhiteSpace(ch))
            {
                if (!lastWasSpace && builder.Length > 0)
                {
                    builder.Append(' ');
                }

                lastWasSpace = true;
            }
            else
            {
                builder.Append(ch);
                lastWasSpace = false;
            }
        }

        return builder.ToString().Trim();
    }
}
