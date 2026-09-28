namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>What an <see cref="LtNode"/> stands for in the order-diagram notation.</summary>
public enum LtNodeKind
{
    /// <summary>A state, a page or a stop: a filled ink node.</summary>
    Filled,

    /// <summary>The bottom element, a stop not yet visited, or something absent: the page colour inside a ring.</summary>
    Hollow,

    /// <summary>One of two writes neither of which is above the other: a node in the concurrent colour.</summary>
    Concurrent,

    /// <summary>
    /// The join, or "you are here": the marker, always inside a ring so it
    /// survives greyscale and the paler paper marker (the Marker Is Never Alone Rule).
    /// </summary>
    Join,
}
