namespace Orleans.Lattice.Primitives;

/// <summary>
/// Monotonically advancing lifecycle state for a B+ tree node split operation.
/// The lattice order is Unsplit &lt; SplitInProgress &lt; SplitComplete.
/// Merge is simply <c>max</c> — once a node reaches a higher state it can never go back.
/// </summary>
[GenerateSerializer]
internal enum SplitState
{
    Unsplit = 0,
    SplitInProgress = 1,
    SplitComplete = 2
}

