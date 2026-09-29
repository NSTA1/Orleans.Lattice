namespace Orleans.Lattice.Replication;

/// <summary>
/// The per-tree replication-configuration record stored as the value of the
/// replicated <see cref="LatticeSystemTreeNames.ReplicationConfig"/> OR-Map,
/// keyed by the target tree id. It is a small composite CRDT so that a
/// configuration change authored on one cluster converges across every enrolled
/// peer with the correct conflict semantics per field:
/// <list type="bullet">
/// <item>
/// <description>
/// <b>Enablement</b> - whether the target tree replicates - is an
/// <see cref="RwFlag"/> (<see cref="LatticeMergeMode.RwFlag"/>, disable-wins):
/// a deliberate operator disable wins a concurrent stale enable and is never
/// silently resurrected, the correct bias for a data-egress toggle.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Merge mode</b> - the wire <see cref="LatticeMergeMode"/> the target tree
/// replicates under - is an <see cref="MvRegister"/>
/// (<see cref="LatticeMergeMode.MvRegister"/>). It is deliberately <i>not</i> a
/// last-writer-wins register: concurrent divergent mode assignments from
/// different clusters survive as distinct dot-tagged values
/// (<see cref="HasAmbiguousMode"/>) so a reader can detect the ambiguity and
/// fail closed rather than silently choosing one mode and dead-lettering the
/// loser's data. Concurrent assignments of the <i>same</i> mode also survive as
/// separate values, but they agree and are not ambiguous, so two regions that
/// each enable a tree under one mode converge on that mode.
/// </description>
/// </item>
/// </list>
/// </summary>
/// <remarks>
/// <para>
/// The record is a genuine composite CRDT: <see cref="MergeFrom"/> recurses
/// pointwise into each field CRDT, <see cref="IsBottom"/> is the conjunction of
/// the fields' bottoms, and <see cref="Clone"/> deep-copies each field. This
/// lets it be the value type of an <see cref="OrMap{TKey, TValue}"/> (registered
/// via <c>AddOrMapShape&lt;string, LatticeReplicationConfigEntry&gt;()</c>),
/// whose own merge folds concurrent same-key writes through this type's
/// <see cref="ICrdt{TSelf}.MergeFrom"/>.
/// </para>
/// <para>
/// <b>Extensibility seam.</b> Additional per-tree replication settings (for
/// example residency filters) can be added later as new <c>[Id(n)]</c> field
/// CRDTs; <see cref="MergeFrom"/>, <see cref="IsBottom"/>, and
/// <see cref="Clone"/> already compose field-wise, so a new field folds in
/// without changing the merge contract. No such field is defined yet, so none
/// ships in the wire format.
/// </para>
/// <para>
/// A disabled tree keeps a live <see cref="Mode"/>, so <see cref="IsBottom"/>
/// stays <see langword="false"/> and the entry remains in the OR-Map. Disable
/// removes the resolved runtime mode without forgetting the tree's fixed mode; an
/// already-active shipper is not torn down by this entry. The entry becomes bottom
/// only when both the flag and the register carry no live state.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LatticeReplicationConfigEntry)]
public sealed class LatticeReplicationConfigEntry : ICrdt<LatticeReplicationConfigEntry>
{
    /// <summary>
    /// The disable-wins enablement flag. The target tree replicates only when
    /// <see cref="RwFlag.IsEnabled"/> is <see langword="true"/>.
    /// </summary>
    [Id(0)]
    public RwFlag Enabled { get; set; } = new();

    /// <summary>
    /// The multi-value register holding the target tree's declared wire
    /// <see cref="LatticeMergeMode"/>, each value encoded via
    /// <see cref="EncodeMode"/>. A single live value is the steady state. Two or
    /// more live values record concurrent mode assignments: when they decode to
    /// the same mode they agree and the tree resolves normally; when they
    /// diverge a reader must fail closed (see <see cref="HasAmbiguousMode"/>).
    /// </summary>
    [Id(1)]
    public MvRegister Mode { get; set; } = new();

    /// <summary>
    /// Returns <see langword="true"/> when the target tree is currently enabled
    /// for replication (at least one enable dot and no surviving disable dot).
    /// </summary>
    public bool IsEnabled => Enabled.IsEnabled;

    /// <summary>
    /// Returns <see langword="true"/> when <see cref="Mode"/> carries live values
    /// that decode to more than one <b>distinct</b> merge mode, i.e. concurrent
    /// clusters assigned divergent merge modes that have not been reconciled.
    /// Concurrent assignments of the <i>same</i> mode - two regions each enabling
    /// the tree under the mode the other also chose - leave several live values
    /// in the register but agree, so they are not ambiguous. Readers fail closed
    /// on an ambiguous mode: the merge-mode resolver returns no mode for the tree
    /// (it never picks one of the divergent values) and commit-time shipper
    /// nudges stop, but an already-active shipper keeps shipping the tree's new
    /// local writes. An operator resolves the ambiguity by disabling the tree and
    /// re-enabling it under a single mode.
    /// </summary>
    public bool HasAmbiguousMode => ClassifyModes(out _) is ModeAgreement.Divergent;

    /// <summary>
    /// The distinct currently-live declared merge mode(s) for the target tree,
    /// decoded from <see cref="Mode"/> in the register's deterministic value
    /// order. Empty when no mode has been assigned, a single element in the
    /// steady state (including when several concurrent assignments agree on one
    /// mode), or multiple elements when <see cref="HasAmbiguousMode"/> is
    /// <see langword="true"/>.
    /// </summary>
    public IReadOnlyList<LatticeMergeMode> Modes
    {
        get
        {
            var values = Mode.Values();
            var count = values.Count;
            if (count == 0)
            {
                return Array.Empty<LatticeMergeMode>();
            }

            var result = new List<LatticeMergeMode>(count);
            for (var i = 0; i < count; i++)
            {
                var mode = DecodeMode(values[i]);
                if (!result.Contains(mode))
                {
                    result.Add(mode);
                }
            }

            return result;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// The entry is bottom only when both the enablement flag and the mode
    /// register are bottom - i.e. it carries no live enablement and no live
    /// mode. A disabled-but-still-configured tree is therefore <b>not</b> bottom
    /// (its mode survives), so the containing <see cref="OrMap{TKey, TValue}"/>
    /// retains the slot rather than dropping the tree.
    /// </remarks>
    public bool IsBottom => Enabled.IsBottom && Mode.IsBottom;

    /// <summary>
    /// Enables the target tree for replication, minting a fresh enable dot on
    /// <paramref name="replicaId"/> and cancelling every disable dot currently
    /// observed. A concurrent disable this enable has not observed still wins
    /// (remove-wins).
    /// </summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the enable dot.</param>
    public void Enable(string replicaId, long counter) => Enabled.Enable(replicaId, counter);

    /// <summary>
    /// Disables replication for the target tree, minting a fresh disable dot on
    /// <paramref name="replicaId"/>. The disable dominates any concurrent enable
    /// that has not observed it.
    /// </summary>
    /// <param name="replicaId">The replica authoring the disable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the disable dot.</param>
    public void Disable(string replicaId, long counter) => Enabled.Disable(replicaId, counter);

    /// <summary>
    /// Assigns the target tree's declared wire <paramref name="mode"/>, writing
    /// it into <see cref="Mode"/> from <paramref name="replicaId"/>. A write
    /// supersedes every mode value the writer has already observed; a concurrent
    /// divergent write from another replica survives the next merge and produces
    /// an ambiguous state.
    /// </summary>
    /// <param name="replicaId">The replica authoring the mode assignment. Must be non-empty.</param>
    /// <param name="mode">The wire merge mode the target tree replicates under.</param>
    public void SetMode(string replicaId, LatticeMergeMode mode) => Mode.Set(replicaId, EncodeMode(mode));

    /// <summary>
    /// Reads the unambiguous declared merge mode. Returns <see langword="true"/>
    /// and sets <paramref name="mode"/> when every live value decodes to the same
    /// mode (one value in the steady state, or several concurrent assignments
    /// that agree); returns <see langword="false"/> (with <paramref name="mode"/>
    /// set to its default) when no mode has been assigned or when the mode is
    /// ambiguous (<see cref="HasAmbiguousMode"/>).
    /// </summary>
    /// <param name="mode">The resolved merge mode when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when a single unambiguous mode is present.</returns>
    public bool TryGetMode(out LatticeMergeMode mode)
        => ClassifyModes(out mode) is ModeAgreement.Single;

    /// <summary>
    /// Classifies the register's live values without allocating: no value, one
    /// distinct mode (possibly carried by several concurrent dots), or divergent
    /// modes. Walks <see cref="MvRegister.Entries"/> directly rather than
    /// <see cref="MvRegister.Values"/>, which copies and sorts every value.
    /// </summary>
    private ModeAgreement ClassifyModes(out LatticeMergeMode mode)
    {
        var entries = Mode.Entries;
        if (entries.Count == 0)
        {
            mode = default;
            return ModeAgreement.None;
        }

        var first = DecodeMode(entries[0].Value);
        for (var i = 1; i < entries.Count; i++)
        {
            if (DecodeMode(entries[i].Value) != first)
            {
                mode = default;
                return ModeAgreement.Divergent;
            }
        }

        mode = first;
        return ModeAgreement.Single;
    }

    private enum ModeAgreement
    {
        None,
        Single,
        Divergent,
    }

    /// <inheritdoc />
    public void MergeFrom(LatticeReplicationConfigEntry other)
    {
        ArgumentNullException.ThrowIfNull(other);
        Enabled.MergeFrom(other.Enabled);
        Mode.MergeFrom(other.Mode);
    }

    /// <inheritdoc />
    public LatticeReplicationConfigEntry Clone() => new()
    {
        Enabled = Enabled.Clone(),
        Mode = Mode.Clone(),
    };

    /// <summary>
    /// Encodes a <see cref="LatticeMergeMode"/> as the opaque value bytes stored
    /// in <see cref="Mode"/>. The encoding is a single byte holding the enum's
    /// ordinal value, the inverse of <see cref="DecodeMode"/>.
    /// </summary>
    /// <param name="mode">The merge mode to encode.</param>
    /// <returns>The one-byte value payload.</returns>
    public static byte[] EncodeMode(LatticeMergeMode mode) => [(byte)mode];

    /// <summary>
    /// Decodes the value bytes stored in <see cref="Mode"/> back into a
    /// <see cref="LatticeMergeMode"/>, the inverse of <see cref="EncodeMode"/>.
    /// </summary>
    /// <param name="value">The one-byte value payload. Must be non-empty.</param>
    /// <returns>The decoded merge mode.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="value"/> is empty.</exception>
    public static LatticeMergeMode DecodeMode(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length == 0)
        {
            throw new ArgumentException("An encoded merge mode must carry at least one byte.", nameof(value));
        }

        return (LatticeMergeMode)value[0];
    }
}
