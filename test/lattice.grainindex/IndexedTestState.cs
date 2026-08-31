namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// A status enum used to exercise the payload-predicate fallback: an enum has
/// no order-preserving key encoding in v1, so its entries share one stable key
/// per grain and are matched by reading the payload.
/// </summary>
public enum TestStatus
{
    /// <summary>The default member, so <c>default(TestStatus)</c> is meaningful.</summary>
    Unknown = 0,

    /// <summary>An active subject.</summary>
    Active = 1,

    /// <summary>A retired subject.</summary>
    Retired = 2,
}

/// <summary>
/// The grain state the projection tests project from. It deliberately spans the
/// encoding cases the projector has to tell apart: an ordered value type, an
/// ordered reference type, an ordered nullable, and two types with no
/// order-preserving encoding.
/// </summary>
public sealed class IndexedTestState
{
    /// <summary>An ordered value-type property.</summary>
    public int Age { get; set; }

    /// <summary>An ordered reference-type property.</summary>
    public string Country { get; set; } = string.Empty;

    /// <summary>An ordered nullable property.</summary>
    public DateTimeOffset? LastSeen { get; set; }

    /// <summary>An ordered boolean property.</summary>
    public bool IsActive { get; set; }

    /// <summary>An ordered floating-point property.</summary>
    public double Score { get; set; }

    /// <summary>A property whose type has no order-preserving encoding.</summary>
    public TestStatus Status { get; set; }

    /// <summary>A second property whose type has no order-preserving encoding.</summary>
    public Guid Tenant { get; set; }

    /// <summary>A property no index includes, so exclusion stays observable.</summary>
    public string Secret { get; set; } = string.Empty;
}
