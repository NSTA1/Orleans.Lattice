using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// Resolves the <see cref="ICrdtProvenanceDecoder"/> for a CRDT shape so a
/// consumer (e.g. the State API) can decode an opaque CRDT value into a
/// member-change timeline without knowing the concrete primitive type. Decoders
/// are keyed by <see cref="LatticeMergeMode"/> and, equivalently, by the shape
/// tag string surfaced on a decoded entry (the mode's
/// <see cref="System.Enum.ToString()"/> form, e.g. <c>"OrSet"</c>).
/// <para>
/// The registry is immutable after construction and free of Orleans grain
/// dependencies, so it can be constructed once and shared (the
/// <see cref="Default"/> instance), or built with a custom decoder set. Every
/// typed CRDT shape has a decoder in <see cref="Default"/>; the non-CRDT
/// <see cref="LatticeMergeMode.LwwRegister"/> shape has no member-level
/// provenance and therefore resolves to <see langword="false"/> (its
/// value-over-time timeline is covered elsewhere).
/// </para>
/// </summary>
public sealed class CrdtProvenanceDecoderRegistry
{
    private readonly Dictionary<LatticeMergeMode, ICrdtProvenanceDecoder> _byMode;

    /// <summary>
    /// Initialises a registry over <paramref name="decoders"/>. The last
    /// decoder wins if two share a <see cref="ICrdtProvenanceDecoder.Mode"/>.
    /// </summary>
    /// <param name="decoders">The decoders to register.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="decoders"/> or any element is <see langword="null"/>.
    /// </exception>
    public CrdtProvenanceDecoderRegistry(IEnumerable<ICrdtProvenanceDecoder> decoders)
    {
        ArgumentNullException.ThrowIfNull(decoders);
        _byMode = new Dictionary<LatticeMergeMode, ICrdtProvenanceDecoder>();
        foreach (var decoder in decoders)
        {
            ArgumentNullException.ThrowIfNull(decoder);
            _byMode[decoder.Mode] = decoder;
        }
    }

    /// <summary>
    /// The default registry, carrying every built-in decoder: one per typed
    /// CRDT shape. The non-CRDT <see cref="LatticeMergeMode.LwwRegister"/> shape
    /// has no decoder.
    /// </summary>
    public static CrdtProvenanceDecoderRegistry Default { get; } =
        new(new ICrdtProvenanceDecoder[]
        {
            OrSetProvenanceDecoder.Instance,
            PnCounterProvenanceDecoder.Instance,
            VersionVectorProvenanceDecoder.Instance,
            MvRegisterProvenanceDecoder.Instance,
            OrMapProvenanceDecoder.Instance,
            SequenceProvenanceDecoder.Instance,
            OrFlagProvenanceDecoder.Instance,
            RwFlagProvenanceDecoder.Instance,
            GCounterProvenanceDecoder.Instance,
            GSetProvenanceDecoder.Instance,
            RwSetProvenanceDecoder.Instance,
            MaxRegisterProvenanceDecoder.Instance,
            MinRegisterProvenanceDecoder.Instance,
        });

    /// <summary>
    /// Resolves the decoder for <paramref name="mode"/>.
    /// </summary>
    /// <param name="mode">The CRDT shape to resolve.</param>
    /// <param name="decoder">The resolved decoder when one is registered.</param>
    /// <returns><see langword="true"/> when a decoder is registered for the mode.</returns>
    public bool TryGet(LatticeMergeMode mode, out ICrdtProvenanceDecoder decoder) =>
        _byMode.TryGetValue(mode, out decoder!);

    /// <summary>
    /// Resolves the decoder for the shape tag <paramref name="shape"/> (the
    /// string surfaced on a decoded entry, e.g. <c>"OrSet"</c>). An unrecognised
    /// or non-CRDT shape (including <see langword="null"/>) yields
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="shape">The shape tag string, or <see langword="null"/>.</param>
    /// <param name="decoder">The resolved decoder when one is registered.</param>
    /// <returns><see langword="true"/> when a decoder is registered for the shape.</returns>
    public bool TryGet(string? shape, out ICrdtProvenanceDecoder decoder)
    {
        // The shape tag is defined as the mode's Enum.ToString() form (e.g.
        // "OrSet"), so accept only an exact enum-member name. Enum.TryParse on its
        // own also accepts a numeric ordinal string ("3"), a comma-separated
        // combination ("OrSet,GSet", which folds to the defined RwSet ordinal and
        // would resolve the wrong decoder), and surrounding whitespace - none of
        // which is a shape tag. A round-trip name-equality check rejects them while
        // leaving every genuine tag resolvable.
        if (shape is not null
            && Enum.TryParse<LatticeMergeMode>(shape, out var mode)
            && Enum.IsDefined(mode)
            && string.Equals(mode.ToString(), shape, StringComparison.Ordinal))
        {
            return _byMode.TryGetValue(mode, out decoder!);
        }
        decoder = null!;
        return false;
    }
}
