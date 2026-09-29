using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// One constraint card of the rule builder: what it constrains (a member path,
/// or the whole value / the item when empty), which kind of constraint, and that
/// kind's fields as typed. Mutable and owned by one builder; a card is compiled
/// to the policy model by <see cref="SchemaCardCompiler"/> and recovered from it
/// by <see cref="SchemaCardDecompiler"/>.
/// </summary>
internal sealed class SchemaRuleCard
{
    private static long _next;

    /// <summary>A stable id for this card within the circuit, used to key its markup.</summary>
    public string Id { get; } = "c" + Interlocked.Increment(ref _next).ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>The kind of constraint.</summary>
    public SchemaCardKind Kind { get; set; }

    /// <summary>
    /// The dotted member path the card constrains; empty for the whole value, or,
    /// inside an every-item card, for the item itself.
    /// </summary>
    public string Path { get; set; } = string.Empty;

    /// <summary>Whether a missing or null member passes rather than fails.</summary>
    public bool Optional { get; set; }

    /// <summary>The message a failing value is reported with; empty for the cluster's default.</summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>For <see cref="SchemaCardKind.Type"/>: the kind of value required.</summary>
    public SchemaValueType ValueType { get; set; }

    /// <summary>
    /// For <see cref="SchemaCardKind.Required"/>: require an object or a list rather
    /// than a scalar, which a presence check on a scalar cannot see.
    /// </summary>
    public bool Structural { get; set; }

    /// <summary>For <see cref="SchemaCardKind.OneOf"/>: the allowed values, as typed.</summary>
    public List<string> Values { get; set; } = [];

    /// <summary>For <see cref="SchemaCardKind.OneOf"/>: compare the values as numbers rather than text.</summary>
    public bool ValuesAreNumbers { get; set; }

    /// <summary>For the range cards: the inclusive minimum, as typed; empty for none.</summary>
    public string Minimum { get; set; } = string.Empty;

    /// <summary>For the range cards: the inclusive maximum, as typed; empty for none.</summary>
    public string Maximum { get; set; } = string.Empty;

    /// <summary>For <see cref="SchemaCardKind.NumberRange"/>: require a whole number.</summary>
    public bool IntegerOnly { get; set; }

    /// <summary>For <see cref="SchemaCardKind.Format"/>: the format.</summary>
    public SchemaTextFormat Format { get; set; }

    /// <summary>For <see cref="SchemaCardKind.TextMatch"/>: where the text must appear.</summary>
    public SchemaTextMatch Match { get; set; }

    /// <summary>For <see cref="SchemaCardKind.TextMatch"/>: the text.</summary>
    public string MatchText { get; set; } = string.Empty;

    /// <summary>For <see cref="SchemaCardKind.Pattern"/>: the regular expression.</summary>
    public string Pattern { get; set; } = string.Empty;

    /// <summary>For <see cref="SchemaCardKind.Encoding"/>: UTF-8 or JSON.</summary>
    public LatticeSchemaEncodingKind Encoding { get; set; } = LatticeSchemaEncodingKind.Json;

    /// <summary>For <see cref="SchemaCardKind.MaxSize"/>: the largest size in bytes, as typed.</summary>
    public string MaxBytes { get; set; } = string.Empty;

    /// <summary>For <see cref="SchemaCardKind.EveryItem"/>: the card every item must satisfy, with a path relative to the item.</summary>
    public SchemaRuleCard? Item { get; set; }

    /// <summary>For <see cref="SchemaCardKind.AnyOf"/>: the alternatives, at least one of which must hold.</summary>
    public List<SchemaRuleCard> Alternatives { get; set; } = [];

    /// <summary>For <see cref="SchemaCardKind.Custom"/>: the rule, kept exactly as it was read.</summary>
    public LatticeSchemaRule? Original { get; set; }

    /// <summary>Whether the card constrains the whole value (or the item) rather than a member.</summary>
    public bool IsWholeValue => Path.Length == 0;

    /// <summary>Creates a card of <paramref name="kind"/> on <paramref name="path"/>.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="path">The member path; empty for the whole value.</param>
    /// <returns>The card.</returns>
    public static SchemaRuleCard Of(SchemaCardKind kind, string path = "") => new() { Kind = kind, Path = path };

    /// <summary>A deep copy with a new id, for editing without touching the original.</summary>
    /// <returns>The copy.</returns>
    public SchemaRuleCard Clone() => new()
    {
        Kind = Kind,
        Path = Path,
        Optional = Optional,
        Description = Description,
        ValueType = ValueType,
        Structural = Structural,
        Values = [.. Values],
        ValuesAreNumbers = ValuesAreNumbers,
        Minimum = Minimum,
        Maximum = Maximum,
        IntegerOnly = IntegerOnly,
        Format = Format,
        Match = Match,
        MatchText = MatchText,
        Pattern = Pattern,
        Encoding = Encoding,
        MaxBytes = MaxBytes,
        Item = Item?.Clone(),
        Alternatives = [.. Alternatives.Select(alternative => alternative.Clone())],
        Original = Original,
    };
}
