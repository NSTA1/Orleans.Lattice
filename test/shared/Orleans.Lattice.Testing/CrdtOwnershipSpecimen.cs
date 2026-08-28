namespace Orleans.Lattice.Testing;

/// <summary>
/// One auditable CRDT type for
/// <see cref="CrdtBufferOwnershipContractTestsBase"/>: how to build a populated
/// and an empty instance, how (optionally) to author and apply a typed delta, and
/// which public projections hand <see cref="byte"/>[] payloads back to a caller.
/// <para>
/// The consuming test project supplies these because only it has a compile-time
/// reference to the product types; the base drives them through reflection and
/// object-graph inspection, so it stays product-agnostic.
/// </para>
/// </summary>
/// <param name="CrdtType">
/// The audited type. A constructed generic (for example
/// <c>OrMap&lt;string, Rga&gt;</c>) covers its generic definition.
/// </param>
/// <param name="CreatePopulated">
/// Builds an instance carrying at least one non-empty <see cref="byte"/>[]
/// payload. An all-empty specimen would pass every leg vacuously.
/// </param>
/// <param name="CreateEmpty">Builds a fresh instance to fold into.</param>
/// <param name="Projections">
/// The public projections that return <see cref="byte"/>[] payloads. Must cover
/// every such method on the type, which
/// <see cref="CrdtBufferOwnershipContractTestsBase.Every_public_byte_array_projection_is_registered"/>
/// enforces.
/// </param>
/// <param name="CreateDeltaFrom">
/// Authors a typed delta from a populated instance, or <see langword="null"/> when
/// the type has no delta fold.
/// </param>
/// <param name="ApplyDelta">
/// Folds a delta authored by <paramref name="CreateDeltaFrom"/> into a receiver.
/// </param>
/// <param name="Label">
/// Optional discriminator when one type registers several specimens (for example
/// a composite with one contributor and with several).
/// </param>
/// <param name="PayloadFree">
/// Declares that the type retains no caller <see cref="byte"/>[] at all - the
/// set primitives encode elements as base64 strings, so no caller array is ever
/// held and the ownership legs are satisfied by construction. The guard verifies
/// the claim both ways: a payload-free specimen must expose no reachable buffer,
/// and any other specimen must expose at least one (so it cannot pass vacuously).
/// </param>
public sealed record CrdtOwnershipSpecimen(
    Type CrdtType,
    Func<object> CreatePopulated,
    Func<object> CreateEmpty,
    IReadOnlyList<CrdtOwnershipProjection> Projections,
    Func<object, object>? CreateDeltaFrom = null,
    Action<object, object>? ApplyDelta = null,
    string? Label = null,
    bool PayloadFree = false)
{
    /// <summary>A human-readable identity for assertion messages.</summary>
    public string Description => Label is null ? CrdtType.Name : $"{CrdtType.Name} ({Label})";
}

/// <summary>
/// One public projection on a CRDT type that hands <see cref="byte"/>[] payloads
/// back to a caller, and therefore sits on the egress leg of the buffer-ownership
/// contract documented on the product's <c>ICrdt&lt;TSelf&gt;</c>.
/// </summary>
/// <param name="MethodName">
/// The declaring method's name, matched against the type's public parameterless
/// <see cref="byte"/>[]-bearing methods so a newly added projection cannot go
/// unregistered.
/// </param>
/// <param name="Invoke">Calls the projection and yields the returned payloads.</param>
/// <param name="Name">Optional display name; defaults to <paramref name="MethodName"/>.</param>
public sealed record CrdtOwnershipProjection(
    string MethodName,
    Func<object, IEnumerable<byte[]>> Invoke,
    string? Name = null)
{
    /// <summary>A human-readable identity for assertion messages.</summary>
    public string Name { get; init; } = Name ?? MethodName;
}
