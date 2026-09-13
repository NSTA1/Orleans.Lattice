namespace Orleans.Lattice;

/// <summary>
/// Marks an enum whose every member is reported as a distinct value of one
/// metric tag, so that the exhaustive-arming gate can find it by reflection
/// rather than from a hand-maintained list (issue #2939).
/// </summary>
/// <remarks>
/// <para>
/// The property this marker exists to protect is one-directional:
/// <b>every member of a marked enum must have an armed tag; NOT every armed tag
/// need be a member.</b> An instrument's tag space is a superset of the enum it
/// reports, not a bijection with it. On
/// <c>orleans.lattice.wal.gc.blocked_leaf_reactivations</c> the four terminal
/// arms come from <c>ReactivationOutcome</c> while <c>attempted</c>,
/// <c>healed</c>, <c>abandoned</c> and <c>rearmed</c> are lifecycle events that
/// are not enum members at all, and all four are legitimate. A bidirectional
/// gate would fail on them immediately.
/// </para>
/// <para>
/// A marker is needed because nothing in the source distinguishes an enum that
/// is <em>supposed</em> to be fully armed from one that merely happens to be
/// referenced near a metric. The alternative to a marker is a hand-maintained
/// list, which is the rot this gate exists to remove.
/// </para>
/// <para>
/// <b>Every argument here is verified by the gate; none of it is documentation.</b>
/// That is deliberate, and it is the lesson of issue #2938: the damage there was
/// done by descriptive prose that nothing checked, which stayed true-sounding
/// while the code moved underneath it. <see cref="TagMappingDeclaringType"/> must
/// declare exactly one static method taking this enum and returning a tag,
/// <see cref="TagName"/> must equal the key that mapping actually returns, and
/// <see cref="InstrumentName"/> must name an instrument the core assembly really
/// declares. A wrong value in any of the three fails the suite rather than
/// misleading a reader.
/// </para>
/// <para>
/// Applying the marker requires no visibility change. The gate reflects over
/// <see cref="System.Reflection.Assembly.GetTypes"/> and binds with
/// <see cref="System.Reflection.BindingFlags.NonPublic"/>, so a
/// <see langword="private"/> mapping method on an <see langword="internal"/>
/// enum is reachable as-is. Issue #2939 anticipated that a general gate would
/// force every marked enum to widen to <see langword="internal"/>; it does not,
/// and nothing should be widened on this gate's account.
/// </para>
/// </remarks>
/// <param name="tagMappingDeclaringType">
/// The type declaring the static method that maps a member of this enum onto its
/// pre-allocated metric tag.
/// </param>
/// <param name="instrumentName">
/// The dotted name of the instrument carrying the tag, for example
/// <c>orleans.lattice.shard.healing.decisions</c>.
/// </param>
/// <param name="tagName">
/// The tag key the mapping populates, for example <c>decision</c>.
/// </param>
[AttributeUsage(AttributeTargets.Enum, AllowMultiple = false, Inherited = false)]
internal sealed class InstrumentedEnumAttribute(
    Type tagMappingDeclaringType,
    string instrumentName,
    string tagName) : Attribute
{
    /// <summary>The type declaring this enum's tag-mapping method.</summary>
    public Type TagMappingDeclaringType { get; } = tagMappingDeclaringType;

    /// <summary>The dotted name of the instrument carrying the tag.</summary>
    public string InstrumentName { get; } = instrumentName;

    /// <summary>The tag key the mapping populates.</summary>
    public string TagName { get; } = tagName;
}
