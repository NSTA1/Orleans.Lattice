using System.Linq.Expressions;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The typed query entry point for one declared grain index: turns a predicate
/// over a grain's state into a scan of the index tree and hands back the
/// matching grains.
/// <para>Example:</para>
/// <code>
/// var index = provider.GetIndex&lt;IUserGrain, UserState&gt;("users");
/// await foreach (var user in index.Where(u =&gt; u.Age &gt;= 18).ToGrainsAsync())
/// {
///     // ...
/// }
/// </code>
/// </summary>
/// <remarks>
/// <para>
/// <b>Supported predicates.</b> The dialect is the core Lattice predicate
/// dialect - member access on the state parameter, constants and captured
/// locals, the comparison operators <c>== != &lt; &lt;= &gt; &gt;=</c>, the
/// boolean operators <c>&amp;&amp; || !</c>, and the string methods
/// <c>StartsWith</c> / <c>EndsWith</c> / <c>Contains</c> / <c>Equals</c> - so a
/// construct outside it fails with the same <see cref="NotSupportedException"/>
/// it would elsewhere in Lattice. Two extra rules come from the index itself: a
/// predicate may only name properties the index projects (otherwise
/// <see cref="GrainIndexPropertyNotIndexedException"/>), and it may only compare
/// a top-level projected property against a constant, never one property against
/// another.
/// </para>
/// <para>
/// <b>How a predicate is routed.</b> The lambda is normalised into a union of
/// conjunctions. Within a conjunction, each property's comparisons collapse into
/// the ordinal key range its entries occupy - a point range for an equality, a
/// half-open range for an inequality, a prefix range for
/// <c>StartsWith</c> - and anything the range cannot express exactly is left to
/// the tree's own server-side predicate push-down over the entry payload. The
/// most selective clause runs first and seeds the candidate grain keys; every
/// further clause narrows them. Across the union, each grain is yielded once.
/// </para>
/// <para>
/// <b>Why a conjunction is several scans.</b> An index entry carries exactly one
/// projected property, so a single predicate naming two of them matches no entry
/// at all. <c>u =&gt; u.Age &gt;= 18 &amp;&amp; u.Country == "GB"</c> is
/// therefore answered as an <c>Age</c> scan and a <c>Country</c> scan whose grain
/// keys are intersected, not as one wider predicate.
/// </para>
/// <para>
/// <b>Memory.</b> A single-property query streams end to end and buffers
/// nothing. A conjunction across properties buffers the grain keys of its most
/// selective clause, and a union buffers the grain keys already yielded so a
/// grain matching several branches appears once. Result payloads are never
/// accumulated.
/// </para>
/// <para>
/// <b>Consistency.</b> An index entry reflects the last state that was
/// <i>projected</i> into it, which may lag the grain's in-memory state, so a
/// query is eventually consistent with respect to grain state. The result is
/// exact with respect to the index: it is precisely the set of grains whose
/// indexed entries satisfy the predicate.
/// <see cref="GrainIndexQueryExecution.SnapshotCursor"/> additionally pins the
/// index state for the whole scan. The query surface is deliberately free of any
/// promise about grain state itself, which leaves room for an opt-in
/// linearizable mode should Lattice later back grain state directly.
/// </para>
/// </remarks>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public interface IGrainIndex<TGrain, TState>
    where TGrain : IGrain
{
    /// <summary>The index's logical name.</summary>
    string Name { get; }

    /// <summary>
    /// The properties the index projects, in declaration order. A predicate may
    /// only name these.
    /// </summary>
    IReadOnlyList<string> IndexedProperties { get; }

    /// <summary>
    /// Plans a query for <paramref name="predicate"/>. The expression is
    /// translated, validated, and routed here, so an unsupported construct or an
    /// unprojected property fails on this call rather than part-way through a
    /// scan.
    /// </summary>
    /// <param name="predicate">The predicate over grain state. Must not be <c>null</c>.</param>
    /// <returns>The planned query.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The predicate uses a construct outside the supported dialect.</exception>
    /// <exception cref="GrainIndexPropertyNotIndexedException">The predicate names a property the index does not project.</exception>
    IGrainIndexQuery<TGrain> Where(Expression<Func<TState, bool>> predicate);
}
