namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// One vector as the store of record holds it: a caller-supplied string
/// identifier and its components.
/// </summary>
/// <param name="Id">The source's own identifier for the vector.</param>
/// <param name="Vector">
/// The vector's components. The memory is borrowed for the duration of the
/// enumeration step that yielded it - the index copies what it keeps - so a
/// source may reuse one buffer across the whole walk.
/// </param>
public readonly record struct VectorSourceEntry(string Id, ReadOnlyMemory<float> Vector);
