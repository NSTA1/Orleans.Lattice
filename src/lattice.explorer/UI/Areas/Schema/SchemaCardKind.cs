namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The kinds of constraint card the rule builder offers. Each compiles to the
/// policy model - a structured predicate, a pattern or an encoding rule - and a
/// rule the builder cannot map back is shown as <see cref="Custom"/>, kept as it is.
/// </summary>
internal enum SchemaCardKind
{
    /// <summary>The member must be present and not null.</summary>
    Required = 0,

    /// <summary>The member must be text, a number, true or false, an object or a list.</summary>
    Type = 1,

    /// <summary>The member must be one of a fixed set of values.</summary>
    OneOf = 2,

    /// <summary>The member must be a number within a range, optionally a whole number.</summary>
    NumberRange = 3,

    /// <summary>The member must be text whose length is within a range.</summary>
    TextLength = 4,

    /// <summary>The member must be text in a common format (compiles to a pattern).</summary>
    Format = 5,

    /// <summary>The member must start with, end with or contain some text.</summary>
    TextMatch = 6,

    /// <summary>The member must be a list whose item count is within a range.</summary>
    ListLength = 7,

    /// <summary>The member must be a list, every item of which satisfies another card.</summary>
    EveryItem = 8,

    /// <summary>The member, or the whole value, must match a regular expression.</summary>
    Pattern = 9,

    /// <summary>The whole value must be well-formed UTF-8, or one JSON document.</summary>
    Encoding = 10,

    /// <summary>The whole value must be at most a number of bytes.</summary>
    MaxSize = 11,

    /// <summary>At least one of several cards must hold.</summary>
    AnyOf = 12,

    /// <summary>A rule the builder cannot express as a card, kept exactly as it is.</summary>
    Custom = 13,
}
