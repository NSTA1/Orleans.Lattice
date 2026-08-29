using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// A read result for the strict-mode schema dead-letter queue of a single tree: the
/// outcome <see cref="Status"/>, an optional <see cref="Message"/> (populated on a
/// denial or failure), the total <see cref="Count"/>, and a bounded page of
/// <see cref="Entries"/>. The compliance service folds a server denial or a
/// transport failure into a non-success view rather than throwing, so the UI never
/// leaks an exception.
/// </summary>
public sealed record SchemaDeadLetterView
{
    /// <summary>The outcome category of the read.</summary>
    public required SchemaOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The total dead-letter entry count for the tree. Zero on a denial or failure.</summary>
    public int Count { get; init; }

    /// <summary>The bounded page of dead-letter entries read. Empty on a denial or failure.</summary>
    public IReadOnlyList<LatticeSchemaDeadLetterEntry> Entries { get; init; } =
        Array.Empty<LatticeSchemaDeadLetterEntry>();

    /// <summary><see langword="true"/> when the read succeeded.</summary>
    public bool IsSuccess => Status == SchemaOperationStatus.Succeeded;
}
