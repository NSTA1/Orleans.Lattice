using System.Globalization;
using System.Text.Json;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A tree's value shape, inferred from a sample of its values and the member
/// paths its policy already names: a tree of members with the kinds of value
/// seen, examples, ranges, and list items. Bounded in depth, breadth and size,
/// so a hostile or enormous value cannot make it expensive.
/// </summary>
internal sealed class SchemaShape
{
    /// <summary>How many distinct values a member keeps for the "one of" card.</summary>
    public const int DistinctLimit = 16;

    /// <summary>How deep into a value the shape looks.</summary>
    public const int MaximumDepth = 8;

    /// <summary>How many members of one object the shape keeps.</summary>
    public const int MaximumChildren = 64;

    /// <summary>How many members the whole shape keeps.</summary>
    public const int MaximumNodes = 400;

    /// <summary>How many text values a member keeps for format detection.</summary>
    public const int TextLimit = 24;

    private int _nodes = 1;

    private SchemaShape()
    {
        Root = new SchemaShapeNode(string.Empty, string.Empty, null, isItem: false);
    }

    /// <summary>The whole value.</summary>
    public SchemaShapeNode Root { get; }

    /// <summary>How many values were read.</summary>
    public int Documents { get; private set; }

    /// <summary>How many of those were not JSON.</summary>
    public int NotJson { get; private set; }

    /// <summary>Whether the shape stopped growing at one of its bounds.</summary>
    public bool Truncated { get; private set; }

    /// <summary>Whether nothing is known: no value was read and the policy names no member.</summary>
    public bool IsEmpty => Documents == 0 && Root.Children.Count == 0;

    /// <summary>Infers the shape of <paramref name="values"/>, adding the members <paramref name="rules"/> name.</summary>
    /// <param name="values">The sampled value bodies.</param>
    /// <param name="rules">The rules of the current policy, or none.</param>
    /// <returns>The shape.</returns>
    public static SchemaShape Infer(IEnumerable<byte[]> values, IEnumerable<LatticeSchemaRule> rules)
    {
        ArgumentNullException.ThrowIfNull(values);
        ArgumentNullException.ThrowIfNull(rules);
        var shape = new SchemaShape();
        foreach (var value in values)
        {
            shape.Documents++;
            try
            {
                using var document = JsonDocument.Parse(value);
                shape.Observe(shape.Root, document.RootElement, 0);
            }
            catch (JsonException)
            {
                shape.NotJson++;
            }
        }

        foreach (var rule in rules)
        {
            if (rule.MemberPath is { Length: > 0 } member)
            {
                shape.Name(shape.Root, member);
            }

            if (rule.Predicate is { } predicate)
            {
                shape.NamePaths(shape.Root, predicate, 0);
            }
        }

        return shape;
    }

    /// <summary>Finds the member at a dotted <paramref name="path"/> from the root, if the shape holds it.</summary>
    /// <param name="path">The path.</param>
    /// <returns>The node, or <see langword="null"/>.</returns>
    public SchemaShapeNode? Find(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return Find(Root, path);
    }

    /// <summary>Finds the member at a dotted <paramref name="path"/> below <paramref name="scope"/>.</summary>
    /// <param name="scope">The node paths are relative to: the root or a list's items.</param>
    /// <param name="path">The path; empty for <paramref name="scope"/> itself.</param>
    /// <returns>The node, or <see langword="null"/>.</returns>
    public static SchemaShapeNode? Find(SchemaShapeNode scope, string path)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (path.Length == 0)
        {
            return scope;
        }

        var node = scope;
        foreach (var segment in path.Split('.'))
        {
            var next = node.Children.FirstOrDefault(pair => string.Equals(pair.Key, segment, StringComparison.OrdinalIgnoreCase)).Value;
            if (next is null)
            {
                return null;
            }

            node = next;
        }

        return node;
    }

    /// <summary>
    /// Wraps <paramref name="leaf"/>, a card on <paramref name="node"/>, in the
    /// every-item cards the node's list scopes need, outermost first: a card on
    /// <c>lines[].sku</c> becomes "every item of lines: sku ...".
    /// </summary>
    /// <param name="node">The member the leaf card constrains.</param>
    /// <param name="leaf">The card; its path is set to the node's path within its scope.</param>
    /// <returns>The card to add to the rule set.</returns>
    public static SchemaRuleCard Wrap(SchemaShapeNode node, SchemaRuleCard leaf)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(leaf);
        leaf.Path = node.Path;
        var card = leaf;
        var scopes = node.ItemScopes();
        for (var index = scopes.Count - 1; index >= 0; index--)
        {
            var list = scopes[index].Parent!;
            card = new SchemaRuleCard { Kind = SchemaCardKind.EveryItem, Path = list.Path, Item = card };
        }

        return card;
    }

    private void Observe(SchemaShapeNode node, JsonElement element, int depth)
    {
        node.Seen++;
        switch (element.ValueKind)
        {
            case JsonValueKind.Null:
                node.Nulls++;
                return;

            case JsonValueKind.String:
                Count(node, SchemaValueType.Text);
                var text = element.GetString() ?? string.Empty;
                var length = CountRunes(text);
                node.TextLengths = node.TextLengths is { } lengths ? (Math.Min(lengths.Min, length), Math.Max(lengths.Max, length)) : (length, length);
                node.AddDistinct(text);
                if (node.Texts.Count < TextLimit && !node.Texts.Contains(text, StringComparer.Ordinal))
                {
                    node.Texts.Add(text);
                }

                return;

            case JsonValueKind.Number:
                Count(node, SchemaValueType.Number);
                var number = element.GetDouble();
                node.Numbers = node.Numbers is { } range ? (Math.Min(range.Min, number), Math.Max(range.Max, number)) : (number, number);
                node.AllWhole &= element.TryGetInt64(out _) || number == Math.Truncate(number);
                node.AddDistinct(element.GetRawText());
                return;

            case JsonValueKind.True:
            case JsonValueKind.False:
                Count(node, SchemaValueType.Boolean);
                node.AddDistinct(element.ValueKind == JsonValueKind.True ? "true" : "false");
                return;

            case JsonValueKind.Array:
                Count(node, SchemaValueType.List);
                var items = element.GetArrayLength();
                node.ItemCounts = node.ItemCounts is { } counts ? (Math.Min(counts.Min, items), Math.Max(counts.Max, items)) : (items, items);
                if (depth >= MaximumDepth)
                {
                    Truncated = true;
                    return;
                }

                if (node.Items is null && !TryGrow())
                {
                    return;
                }

                node.Items ??= new SchemaShapeNode("each item", string.Empty, node, isItem: true);
                foreach (var item in element.EnumerateArray())
                {
                    Observe(node.Items, item, depth + 1);
                }

                return;

            case JsonValueKind.Object:
                Count(node, SchemaValueType.Object);
                if (depth >= MaximumDepth)
                {
                    Truncated = true;
                    return;
                }

                foreach (var property in element.EnumerateObject())
                {
                    // A dotted name cannot be addressed by a member path, so it is left out.
                    if (property.Name.Length == 0 || property.Name.Contains('.', StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (Child(node, property.Name) is { } child)
                    {
                        Observe(child, property.Value, depth + 1);
                    }
                }

                return;
        }
    }

    private void NamePaths(SchemaShapeNode scope, LatticePredicateNode node, int depth)
    {
        if (depth > SchemaPredicateText.MaximumDepth)
        {
            return;
        }

        var path = node.MemberPath;
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Member:
            case LatticePredicateNodeKind.TypeOf:
            case LatticePredicateNodeKind.Length:
                if (!string.IsNullOrEmpty(path))
                {
                    Name(scope, path);
                }

                break;

            case LatticePredicateNodeKind.Every when node.Children is [var body]:
                var list = string.IsNullOrEmpty(path) ? scope : Name(scope, path);
                if (list is not null)
                {
                    if (list.Items is null && TryGrow())
                    {
                        list.Items = new SchemaShapeNode("each item", string.Empty, list, isItem: true);
                    }

                    if (list.Items is { } items)
                    {
                        NamePaths(items, body, depth + 1);
                    }
                }

                return;
        }

        foreach (var child in node.Children ?? [])
        {
            NamePaths(scope, child, depth + 1);
        }
    }

    private SchemaShapeNode? Name(SchemaShapeNode scope, string path)
    {
        var node = scope;
        foreach (var segment in path.Split('.'))
        {
            if (segment.Length == 0)
            {
                return null;
            }

            var existing = node.Children.FirstOrDefault(pair => string.Equals(pair.Key, segment, StringComparison.OrdinalIgnoreCase)).Value;
            node = existing ?? Child(node, segment);
            if (node is null)
            {
                return null;
            }
        }

        node.NamedByPolicy = true;
        return node;
    }

    private SchemaShapeNode? Child(SchemaShapeNode parent, string name)
    {
        if (parent.Children.TryGetValue(name, out var child))
        {
            return child;
        }

        if (parent.Children.Count >= MaximumChildren || !TryGrow())
        {
            Truncated = true;
            return null;
        }

        child = new SchemaShapeNode(name, parent.Path.Length == 0 ? name : parent.Path + "." + name, parent, isItem: false);
        parent.Children.Add(name, child);
        return child;
    }

    private bool TryGrow()
    {
        if (_nodes >= MaximumNodes)
        {
            Truncated = true;
            return false;
        }

        _nodes++;
        return true;
    }

    private static void Count(SchemaShapeNode node, SchemaValueType type) =>
        node.Types[type] = node.Types.GetValueOrDefault(type) + 1;

    private static int CountRunes(string text)
    {
        var count = 0;
        foreach (var _ in text.EnumerateRunes())
        {
            count++;
        }

        return count;
    }

    /// <summary>A number as the shape shows it.</summary>
    /// <param name="value">The number.</param>
    /// <returns>The text.</returns>
    internal static string Show(double value) => value.ToString("#,0.###", CultureInfo.InvariantCulture);
}
