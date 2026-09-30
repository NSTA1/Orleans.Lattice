namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// One member of a tree's inferred shape: what it is called, how to address it,
/// the kinds of value seen there and how often, examples, the observed ranges,
/// and its own members or list items. Built by <see cref="SchemaShape"/> from a
/// sample of values and the policy's own member paths.
/// </summary>
internal sealed class SchemaShapeNode
{
    private readonly Dictionary<string, int> _distinct = new(StringComparer.Ordinal);

    /// <summary>Creates a node.</summary>
    /// <param name="name">The member name shown; "each item" for a list's items, or empty for the root.</param>
    /// <param name="path">The dotted path from the nearest scope root (the value, or a list item).</param>
    /// <param name="parent">The enclosing node, or <see langword="null"/> for the root.</param>
    /// <param name="isItem">Whether this node stands for the items of its parent list.</param>
    public SchemaShapeNode(string name, string path, SchemaShapeNode? parent, bool isItem)
    {
        Name = name;
        Path = path;
        Parent = parent;
        IsItem = isItem;
    }

    /// <summary>The name shown.</summary>
    public string Name { get; }

    /// <summary>The dotted path from the nearest scope root: the whole value, or the list item this node sits in.</summary>
    public string Path { get; }

    /// <summary>The enclosing node.</summary>
    public SchemaShapeNode? Parent { get; }

    /// <summary>Whether this node stands for the items of its parent list.</summary>
    public bool IsItem { get; }

    /// <summary>How many sampled documents (or items) held this member at all.</summary>
    public int Seen { get; set; }

    /// <summary>How many of those held JSON null.</summary>
    public int Nulls { get; set; }

    /// <summary>How often each kind of value was seen.</summary>
    public Dictionary<SchemaValueType, int> Types { get; } = [];

    /// <summary>The smallest and largest number seen.</summary>
    public (double Min, double Max)? Numbers { get; set; }

    /// <summary>Whether every number seen was whole.</summary>
    public bool AllWhole { get; set; } = true;

    /// <summary>The shortest and longest text seen, in characters.</summary>
    public (int Min, int Max)? TextLengths { get; set; }

    /// <summary>The fewest and most list items seen.</summary>
    public (int Min, int Max)? ItemCounts { get; set; }

    /// <summary>Whether a rule of the current policy names this member.</summary>
    public bool NamedByPolicy { get; set; }

    /// <summary>The members of an object seen here, by name.</summary>
    public SortedDictionary<string, SchemaShapeNode> Children { get; } = new(StringComparer.Ordinal);

    /// <summary>The shape of this list's items, when a list was seen here.</summary>
    public SchemaShapeNode? Items { get; set; }

    /// <summary>Whether more distinct scalar values were seen than are kept.</summary>
    public bool ManyValues { get; private set; }

    /// <summary>The distinct scalar values seen, most frequent first, at most <see cref="SchemaShape.DistinctLimit"/>.</summary>
    public IReadOnlyList<string> Distinct =>
        [.. _distinct.OrderByDescending(pair => pair.Value).ThenBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => pair.Key)];

    /// <summary>The distinct text values seen, for format detection.</summary>
    public List<string> Texts { get; } = [];

    /// <summary>The kind of value seen most, or <see langword="null"/> when none was.</summary>
    public SchemaValueType? Dominant =>
        Types.Count == 0 ? null : Types.OrderByDescending(pair => pair.Value).ThenBy(pair => pair.Key).First().Key;

    /// <summary>Whether no sample showed this member: it is known only from the policy.</summary>
    public bool Unseen => Seen == 0;

    /// <summary>A stable id for markup, unique within one shape.</summary>
    public string DomId => "schema-shape-" + (IsItem ? "i" : "m") + "-" + Uri.EscapeDataString(FullLabel).Replace("%", "_", StringComparison.Ordinal);

    /// <summary>The address as written for people, with "[]" marking list items, such as <c>lines[].sku</c>.</summary>
    public string FullLabel
    {
        get
        {
            if (Parent is null)
            {
                return string.Empty;
            }

            var parent = Parent.FullLabel;
            return IsItem ? parent + "[]" : (parent.Length == 0 ? Name : parent + "." + Name);
        }
    }

    /// <summary>Records one distinct scalar value.</summary>
    /// <param name="value">The value as text.</param>
    public void AddDistinct(string value)
    {
        if (_distinct.TryGetValue(value, out var count))
        {
            _distinct[value] = count + 1;
        }
        else if (_distinct.Count < SchemaShape.DistinctLimit)
        {
            _distinct[value] = 1;
        }
        else
        {
            ManyValues = true;
        }
    }

    /// <summary>The list nodes between the root and this node, outermost first: each is a scope boundary a card must cross with "every item".</summary>
    /// <returns>The chain of list-item nodes.</returns>
    public IReadOnlyList<SchemaShapeNode> ItemScopes()
    {
        var scopes = new List<SchemaShapeNode>();
        for (var node = this; node is not null; node = node.Parent)
        {
            if (node.IsItem)
            {
                scopes.Insert(0, node);
            }
        }

        return scopes;
    }
}
