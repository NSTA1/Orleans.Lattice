using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// The result of a multi-key point read that also reports how many of the
/// requested keys the read-path access gate pruned before fan-out. Returned by
/// <see cref="ILattice.GetManyWithGateAccountingAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// This type exists because a plain <see cref="Dictionary{TKey, TValue}"/> of
/// returned rows cannot express the difference between the two reasons a
/// requested key is missing from it. A key the gate pruned and a key that was
/// never written are byte-identical observations - both are simply absent - so a
/// caller reading coverage from the returned rows classifies an existing entry it
/// is not authorized to see as an entry that does not exist. For the repo-context
/// embedding gap sweep (issue #2277) that misclassification is not benign: the
/// source is re-selected and re-embedded on every pass, forever, with a clean log
/// at every layer.
/// </para>
/// <para>
/// The distinguishing information exists only inside the grain that applied the
/// filter, which is why it is reported from there rather than inferred by the
/// caller: <see cref="PrunedByAccessGate"/> is <c>0</c> exactly when every
/// requested key that is missing from <see cref="Values"/> is a genuine absence.
/// </para>
/// <para>
/// A <b>count</b> and never the identities. A list of pruned keys would name the
/// keys the caller is not authorized to see, turning any multi-get into an
/// authorization oracle - which is a worse defect than the one this type fixes.
/// The count is both the sufficient signal and the minimum disclosure.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GatedMultiReadResult)]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record GatedMultiReadResult
{
    /// <summary>
    /// The values for the requested keys that exist, are not tombstoned, and were
    /// not pruned by the read-path access gate. Never <see langword="null"/>.
    /// </summary>
    [Id(0)] public Dictionary<string, byte[]> Values { get; init; } = [];

    /// <summary>
    /// How many of the requested keys the read-path access gate removed before
    /// fan-out, so their values were never read on the silo. <c>0</c> on the
    /// default ungated path, and <c>0</c> whenever an active gate admits every
    /// requested key.
    /// </summary>
    [Id(1)] public int PrunedByAccessGate { get; init; }

    /// <summary>
    /// <see langword="true"/> when every requested key that is absent from
    /// <see cref="Values"/> is a genuine absence, so a caller may safely read a
    /// missing key as "no such entry". <see langword="false"/> when the gate
    /// pruned at least one key, in which case absence is uninterpretable and the
    /// caller must not classify on it.
    /// </summary>
    public bool IsComplete => PrunedByAccessGate == 0;

    /// <summary>
    /// Compares two results by value: the returned rows compared by key and by
    /// value <em>content</em>, plus the prune count. The compiler-generated record
    /// equality compares the dictionary with
    /// <see cref="EqualityComparer{T}.Default"/>, which is reference equality, so
    /// two structurally identical results - and a result that round-trips through
    /// serialization versus its pre-serialization self - would otherwise never
    /// compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(GatedMultiReadResult? other) =>
        other is not null
        && PrunedByAccessGate == other.PrunedByAccessGate
        && ValuesEqual(Values, other.Values);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(PrunedByAccessGate);
        hash.Add(Values.Count);
        return hash.ToHashCode();
    }

    private static bool ValuesEqual(
        Dictionary<string, byte[]> left, Dictionary<string, byte[]> right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left.Count != right.Count) return false;
        foreach (var (key, value) in left)
        {
            if (!right.TryGetValue(key, out var other)) return false;
            if (!value.AsSpan().SequenceEqual(other)) return false;
        }

        return true;
    }
}
