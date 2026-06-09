namespace Orleans.Lattice;

/// <summary>
/// Capability interface a <see cref="ILatticeSerializer{T}"/> implements to
/// declare that its serialized <c>byte[]</c> form is a navigable JSON document,
/// and is therefore eligible for server-side predicate push-down.
/// <para>
/// The typed predicate overloads on the extension layer check for this
/// capability <b>client-side, before any RPC</b>: a serializer that does not
/// implement it causes the typed override to throw a clear
/// <see cref="System.NotSupportedException"/> rather than shipping an IR the
/// server cannot evaluate. <see cref="JsonLatticeSerializer{T}"/> implements it.
/// </para>
/// <para>
/// This is a pure capability marker: the server never sees the serializer or
/// the value type. It parses the value bytes as a JSON document directly, so
/// the contract a serializer accepts by implementing this interface is simply
/// "my <c>Serialize</c> output is a UTF-8 JSON document whose property names
/// match the value type's member names (ordinal, case-insensitive)".
/// </para>
/// </summary>
public interface ILatticePredicateSerializer
{
}
