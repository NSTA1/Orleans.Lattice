namespace Orleans.Lattice;

/// <summary>
/// Marks a persisted grain-state type that Lattice writes through the
/// Orleans binary serializer rather than the default JSON grain-storage
/// serializer.
/// <para>
/// The default <c>JsonGrainStorageSerializer</c> builds the whole document
/// in a <see cref="System.Text.StringBuilder"/> and then calls
/// <c>ToString()</c>, which materialises the entire payload as one
/// contiguous UTF-16 string before any bytes are written. For a state type
/// whose payload is a large opaque frame - a leaf snapshot, say - that
/// single allocation is roughly 2.7x the frame and must be satisfied
/// contiguously, so it is the allocation that fails first when a silo is
/// replaying a warm volume under memory pressure. Writing the same state
/// through the Orleans binary serializer removes both the base64 inflation
/// and the contiguous UTF-16 intermediate.
/// </para>
/// <para>
/// Opting a type in is a deliberate act, not a blanket switch, because the
/// two serializers do not have identical fidelity: the JSON serializer
/// persists any public property, whereas the Orleans binary serializer
/// persists exactly the members carrying <c>[Id(n)]</c>. A type is safe to
/// mark only when its full state is covered by <c>[Id(n)]</c> members -
/// which is guaranteed in practice for any state type that already crosses
/// a grain-call boundary, since that call round-trips it through the same
/// binary serializer.
/// </para>
/// <para>
/// Marking a type changes only what is written from that point on. Reads
/// stay compatible in both directions: <see cref="LatticeGrainStorageSerializer"/>
/// detects the format of each stored payload, so rows written by an earlier
/// build in JSON are still readable, and rows written in binary stay
/// readable if the type is later unmarked. No migration is required.
/// </para>
/// </summary>
public interface ILatticeBinaryPersistedState
{
}
