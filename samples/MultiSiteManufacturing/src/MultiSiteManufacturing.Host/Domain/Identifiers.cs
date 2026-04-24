namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Part serial number shaped like <c>{family}-{year}-{seq:D5}</c>
/// (e.g. <c>HPT-BLD-S1-2028-00142</c>). The wrapper exists so the rest of
/// the codebase can pass a strongly-typed identifier instead of a bare string.
/// </summary>
/// <param name="Value">The serialised serial-number string.</param>
[GenerateSerializer, Immutable]
public readonly record struct PartSerialNumber([property: Id(0)] string Value)
{
    /// <inheritdoc />
    public override string ToString() => Value;

    /// <summary>Formats a serial from its component parts.</summary>
    public static PartSerialNumber From(PartFamily family, int year, int sequence) =>
        new($"{family.Value}-{year:D4}-{sequence:D5}");
}

/// <summary>Short product-family code (e.g. <c>HPT-BLD-S1</c>).</summary>
[GenerateSerializer, Immutable]
public readonly record struct PartFamily([property: Id(0)] string Value)
{
    /// <inheritdoc />
    public override string ToString() => Value;
}

/// <summary>Operator identifier attached to every fact.</summary>
[GenerateSerializer, Immutable]
public readonly record struct OperatorId([property: Id(0)] string Value)
{
    /// <inheritdoc />
    public override string ToString() => Value;

    /// <summary>The default sample operator used by the seeded inventory and the demo UI.</summary>
    public static OperatorId Demo => new("operator:demo");
}
