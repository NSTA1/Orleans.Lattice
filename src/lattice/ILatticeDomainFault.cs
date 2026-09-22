namespace Orleans.Lattice;

/// <summary>
/// Marks an exception that reports a <b>deliberate domain refusal</b> - a
/// condition the store recognised, decided about, and is telling the caller
/// about on purpose - as distinct from the framework failure whose base type it
/// happens to share.
/// <para>
/// <b>The defect this exists to remove.</b> Most domain exceptions in this
/// library derive from a Base Class Library exception subclass, overwhelmingly
/// <see cref="InvalidOperationException"/>, because that type reads naturally
/// at the throw site. The consequence is invisible at the throw site and lives
/// somewhere else entirely: a broad <c>catch (InvalidOperationException)</c>
/// written to recover from a framework failure will also catch every domain
/// refusal that passes through it, and will then apply remediation chosen for a
/// completely different condition. The handler is not wrong about its own case
/// and the exception is not wrong about its own meaning; the two were simply
/// never introduced. Nothing in the type system connects them, no test fails,
/// and the symptom surfaces far from either party.
/// <see cref="LatticeSaturatedException"/> is the worked example: it reports
/// write-ahead-log back-pressure and asks the caller to slow down, and where it
/// met a routing handler's <c>catch (InvalidOperationException)</c> the response
/// was to discard the routing cache and re-issue the whole fan-out, turning one
/// refusal into work proportional to the shard count at precisely the moment the
/// store had asked for less.
/// </para>
/// <para>
/// <b>Why a marker interface and not a shared base class.</b> Re-parenting these
/// exceptions onto <see cref="Exception"/> would remove the ambiguity at its
/// root, and it is not available: the base type of a public exception is part of
/// the public contract of a released package, so changing it breaks every
/// caller that catches the BCL type on purpose, and the same reasoning already
/// recorded on <see cref="ILatticeLeafUnavailable"/> applies here - a shared base
/// class cannot be imposed on types that deliberately sit on different bases for
/// documented reasons, one of which is Orleans' same-silo deep-copy contract. An
/// interface is purely additive. It changes no base type, breaks no caller,
/// leaves every registered deep copier and the
/// <c>SerializableExceptionDeepCopyContractTests</c> audit exactly as they are,
/// and gives a handler the one thing it previously could not express.
/// </para>
/// <para>
/// <b>What a handler is expected to do with it.</b> A broad catch clause that
/// recovers from a framework failure should decline a domain refusal and let it
/// propagate, because remediation chosen for the framework case is by
/// construction not chosen for this one:
/// <code>
/// catch (InvalidOperationException ex) when (ex is not ILatticeDomainFault)
/// {
///     // Framework failure only. A domain refusal reaches the caller unchanged.
/// }
/// </code>
/// That is a decision, not a formality. Declining is the right default precisely
/// because it is the only option that cannot silently convert a considered
/// refusal into an unrelated recovery. A handler that genuinely does want to act
/// on a domain refusal should catch the concrete exception type by name, where
/// the intent is written down and reviewable.
/// </para>
/// <para>
/// <b>What implementing it commits to.</b> That the exception reports a
/// condition the store decided on and means to report - saturation, a quota, a
/// fence, an expired snapshot, a rejected write, a refusal to act - and not an
/// accident of program state that a caller could have avoided. Implement it on
/// every exception in this package whose base is a foreign exception type, which
/// <c>DomainFaultMarkerContractTests</c> enforces against the live assembly so a
/// newly added exception cannot reintroduce the defect. Do not implement it on a
/// genuine argument or state-validation failure raised against the caller's own
/// misuse, and note it says nothing about whether retrying will help - that is
/// the concrete type's business, and a caller that needs to know must ask the
/// concrete type.
/// </para>
/// </summary>
public interface ILatticeDomainFault
{
}
