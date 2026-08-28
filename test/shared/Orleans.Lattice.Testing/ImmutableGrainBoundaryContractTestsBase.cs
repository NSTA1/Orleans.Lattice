using System.Collections;
using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, product-agnostic guard over the <c>[Immutable]</c> same-silo copy
/// elision at a <b>grain boundary</b>. Orleans serialises a grain argument or
/// result cross-silo, but when the callee is co-located it <em>deep-copies</em>
/// instead - unless the type is marked <c>[Immutable]</c>, in which case the copy
/// is skipped and the receiver is handed the <em>sender's own object</em>. That
/// is sound only while nobody mutates the payload. A type marked
/// <c>[Immutable]</c> that carries a mutable <see cref="byte"/>[] or collection
/// and whose payload the receiver folds <em>in place</em> therefore corrupts the
/// sender's state silently, with no cross-silo equivalent - so it reproduces only
/// under co-location and never in a cross-silo test.
/// <para>
/// The audit that produced this guard found the risk is structurally bounded
/// today: CRDT payloads cross grain boundaries as opaque <see cref="byte"/>[]
/// (the receiver decodes them into a fresh object graph before folding), not as
/// typed CRDT objects, so no shared buffer is ever folded in place. That is a
/// <em>reachability argument</em>, not an invariant - one future grain method
/// taking a typed delta re-opens the whole class silently. This guard converts
/// the argument into an enforced invariant: every <c>[Immutable]</c> type that
/// reaches a grain signature carrying a mutable buffer must be explicitly
/// acknowledged, with a written justification, by the owning package. A newly
/// added one fails <c>build-and-test</c> until somebody decides whether its
/// payload is read-only on the receiving side.
/// </para>
/// <para>
/// This library stays product-agnostic: it references no Orleans type at compile
/// time and matches <c>ImmutableAttribute</c>, <c>IdAttribute</c> and the
/// <c>IGrain</c> marker interface by <em>name</em>, exactly as the serializable
/// exception and grain-key guards do. The base is <see langword="abstract"/> so
/// it is never discovered on its own; the inherited <c>[Test]</c> runs through
/// the concrete subclass in the consuming assembly.
/// </para>
/// </summary>
public abstract class ImmutableGrainBoundaryContractTestsBase
{
    private const string ImmutableAttributeName = "ImmutableAttribute";
    private const string IdAttributeName = "IdAttribute";
    private const string GenerateSerializerAttributeName = "GenerateSerializerAttribute";

    /// <summary>
    /// Simple names of the Orleans marker interfaces that make an interface a
    /// grain-call surface. Matched by name so this library needs no compile-time
    /// Orleans reference.
    /// </summary>
    private static readonly HashSet<string> GrainMarkerInterfaceNames =
        new(StringComparer.Ordinal) { "IGrain", "IAddressable", "IGrainObserver" };

    /// <summary>
    /// Carrier/member pairs (<c>"CarrierType.Member"</c>) where a CRDT is known
    /// to be shared across a skipped copy and the finding is <b>tracked as open
    /// work</b>, mapped to the issue reference. This is deliberately <em>not</em>
    /// the same list as <see cref="AcknowledgedReadOnlyPayloads"/>: an entry here
    /// asserts only that the finding is recorded and triaged, never that it has
    /// been proved safe, so a clean run of this guard is not mistaken for a clean
    /// bill of health. Empty by default.
    /// </summary>
    protected virtual IReadOnlyDictionary<string, string> TrackedCrdtCarrierExemptions =>
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// The package assembly whose grain interfaces are audited. Only interfaces
    /// <em>declared</em> in this assembly are walked, and only payload types
    /// declared in it are audited, so each package audits exactly its own
    /// surface. A framework type reached through a signature (an Orleans
    /// <c>IdSpan</c> or <c>GrainId</c>, say) is skipped: it is immutable by
    /// construction and not the package's to change.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Types the owning package has reviewed and recorded as safe to share
    /// across a same-silo boundary, keyed by <see cref="Type.FullName"/> with the
    /// review outcome as the value. The claim an entry makes is that the payload
    /// is <b>read-only on receipt</b>: the receiving side decodes, hashes,
    /// compares or forwards it, and does not fold it in place or retain it in
    /// mutable durable state.
    /// <para>
    /// This list is the <em>broad</em> half of the guard and is reviewed by
    /// payload shape - an opaque caller-authored buffer, a content digest, a
    /// per-call read model - rather than by re-deriving every call site. The
    /// <em>sharp</em> half, which does not rely on that shape review, is
    /// <see cref="No_crdt_state_type_is_shared_across_a_grain_boundary_by_a_skipped_copy"/>:
    /// the in-place-fold class that actually turns a skipped copy into
    /// corruption is CRDT state, and that is pinned by its own test with its own
    /// list. The two together are what make the shape review sufficient.
    /// </para>
    /// <para>
    /// A justification is required rather than a bare name so the next reader
    /// inherits the reasoning instead of re-deriving it. Default is empty, so a
    /// package that adds its first such type must make a deliberate decision.
    /// </para>
    /// </summary>
    protected virtual IReadOnlyDictionary<string, string> AcknowledgedReadOnlyPayloads =>
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Walks every grain interface declared in <see cref="PackageAssembly"/>,
    /// collects every serializable type reachable through its method parameters
    /// and return values, and asserts that each such type marked
    /// <c>[Immutable]</c> while carrying a mutable <see cref="byte"/>[] or
    /// collection member is present in <see cref="AcknowledgedReadOnlyPayloads"/>.
    /// </summary>
    [Test]
    public void Every_immutable_payload_on_a_grain_boundary_is_acknowledged_read_only()
    {
        var grainInterfaces = GrainInterfaces().ToArray();

        Assert.That(grainInterfaces, Is.Not.Empty,
            $"No grain interfaces were discovered in {PackageAssembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        var acknowledged = AcknowledgedReadOnlyPayloads;
        var offenders = new List<string>();

        foreach (var type in SharedMutablePayloads(grainInterfaces))
        {
            var name = type.FullName ?? type.Name;
            if (acknowledged.ContainsKey(name)) continue;

            offenders.Add($"{name}: [Immutable] with mutable {string.Join(", ", MutableBufferMembers(type))}");
        }

        Assert.That(offenders, Is.Empty,
            "A type marked [Immutable] has its same-silo deep copy skipped, so a co-located receiver is "
            + "handed the sender's own instance. Any such type on a grain boundary that carries a mutable "
            + "byte[] or collection must be audited and then listed in AcknowledgedReadOnlyPayloads with a "
            + "justification recording why the receiving side only ever reads it. If the receiver folds the "
            + "payload in place, or retains it in mutable durable state, the fix is to copy on receipt (or "
            + "drop [Immutable]) rather than to acknowledge it. Unacknowledged:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders.OrderBy(static o => o, StringComparer.Ordinal)));
    }

    /// <summary>
    /// Guards the acknowledgement list itself: an entry naming a type that is no
    /// longer reachable from a grain boundary (or no longer carries a mutable
    /// buffer) is stale and must be removed, so the list cannot quietly decay
    /// into a list of excuses that no longer correspond to real types.
    /// </summary>
    [Test]
    public void Acknowledged_read_only_payloads_are_all_still_reachable()
    {
        var acknowledged = AcknowledgedReadOnlyPayloads;
        if (acknowledged.Count == 0)
        {
            Assert.Pass("No acknowledged payloads to verify.");
            return;
        }

        var live = SharedMutablePayloads(GrainInterfaces())
            .Select(t => t.FullName ?? t.Name)
            .ToHashSet(StringComparer.Ordinal);

        var stale = acknowledged.Keys.Where(k => !live.Contains(k)).OrderBy(static k => k, StringComparer.Ordinal).ToArray();

        Assert.That(stale, Is.Empty,
            "These acknowledged payload types are no longer [Immutable]-with-a-mutable-buffer on a grain "
            + "boundary in this package, so their entries are stale and should be deleted:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, stale));

        var blank = acknowledged
            .Where(static kv => string.IsNullOrWhiteSpace(kv.Value))
            .Select(static kv => kv.Key)
            .OrderBy(static k => k, StringComparer.Ordinal)
            .ToArray();

        Assert.That(blank, Is.Empty,
            "An acknowledgement must carry a written justification, not an empty string:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, blank));
    }

    /// <summary>
    /// The sharp invariant the wider sweep's conclusion rests on: <b>no CRDT
    /// state or delta type may be shared across a grain boundary by a skipped
    /// copy.</b> CRDT payloads cross as opaque <see cref="byte"/>[] and are
    /// decoded into a fresh object graph before being folded, so the receiver
    /// never folds a buffer the sender still owns. That is what makes the
    /// remaining <c>[Immutable]</c> payloads safe to acknowledge as read-only
    /// rather than audit one by one - and it is a property of today's signatures,
    /// not a guarantee, so it is pinned here.
    /// <para>
    /// The danger criterion is precisely the <em>skipped copy</em>, so this
    /// flags a CRDT type only where the copy would actually be elided: the CRDT
    /// is itself <c>[Immutable]</c>, or it hangs off an <c>[Immutable]</c>
    /// carrier, whose skipped copy shares the carrier's reference-typed members
    /// too. A CRDT that crosses a boundary <em>without</em> <c>[Immutable]</c>
    /// anywhere above it (<c>VersionVector</c> being the live example) is
    /// deep-copied by Orleans and is safe by construction.
    /// </para>
    /// </summary>
    [Test]
    public void No_crdt_state_type_is_shared_across_a_grain_boundary_by_a_skipped_copy()
    {
        var reachable = ReachablePayloadTypes(GrainInterfaces());
        var tracked = TrackedCrdtCarrierExemptions;
        var offenders = new List<string>();

        foreach (var type in reachable)
        {
            if (IsCrdtStateType(type) && HasAttributeNamed(type, ImmutableAttributeName))
            {
                offenders.Add($"{FriendlyName(type)} is [Immutable] and is a CRDT state type");
            }

            if (!HasAttributeNamed(type, ImmutableAttributeName)) continue;

            foreach (var member in SerializedMembers(type))
            {
                foreach (var carried in Unwrap(MemberType(member)).Where(IsCrdtStateType))
                {
                    if (tracked.ContainsKey($"{type.Name}.{member.Name}")) continue;

                    offenders.Add(
                        $"{FriendlyName(type)}.{member.Name} carries CRDT state {FriendlyName(carried)} "
                        + "inside an [Immutable] carrier");
                }
            }
        }

        Assert.That(offenders.Distinct().OrderBy(static o => o, StringComparer.Ordinal), Is.Empty,
            "A CRDT state or delta type must not be shared across a grain boundary by an elided copy. Orleans "
            + "skips the same-silo deep copy for an [Immutable] payload and hands the receiver the sender's own "
            + "instance - including the reference-typed members of an [Immutable] carrier - and a CRDT receiver "
            + "folds its payload IN PLACE, which silently corrupts the sender's state with no cross-silo "
            + "equivalent to catch it in test. Ship the CRDT as encoded bytes and decode it on the receiving "
            + "side (the established ApplyCrdtDeltaItem.Delta shape), or drop [Immutable] from the carrier so "
            + "the runtime copies it. Offenders:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders.Distinct().OrderBy(static o => o, StringComparer.Ordinal)));
    }

    /// <summary>
    /// Whether <paramref name="type"/> is a CRDT state type - one implementing
    /// the <c>ICrdt&lt;TSelf&gt;</c> contract, matched by name so this library
    /// needs no compile-time product reference - or a typed CRDT delta DTO
    /// carried alongside one.
    /// </summary>
    private static bool IsCrdtStateType(Type type) =>
        type.GetInterfaces().Any(static i =>
            i.IsGenericType && i.Name.StartsWith("ICrdt`", StringComparison.Ordinal));

    /// <summary>
    /// The types this package must answer for: declared in
    /// <see cref="PackageAssembly"/>, reachable from one of its grain
    /// signatures, marked <c>[Immutable]</c>, and carrying at least one mutable
    /// buffer member.
    /// </summary>
    private IEnumerable<Type> SharedMutablePayloads(IEnumerable<Type> grainInterfaces) =>
        ReachablePayloadTypes(grainInterfaces)
            .Where(t => t.Assembly == PackageAssembly
                && HasAttributeNamed(t, ImmutableAttributeName)
                && MutableBufferMembers(t).Any())
            .OrderBy(static t => t.FullName, StringComparer.Ordinal);

    /// <summary>
    /// The grain-call interfaces declared in <see cref="PackageAssembly"/>: any
    /// interface transitively implementing an Orleans grain marker interface.
    /// </summary>
    protected IEnumerable<Type> GrainInterfaces() =>
        PackageAssembly.GetTypes()
            .Where(static t => t.IsInterface
                && !t.ContainsGenericParameters
                && t.GetInterfaces().Any(static i => GrainMarkerInterfaceNames.Contains(i.Name)))
            .OrderBy(static t => t.FullName, StringComparer.Ordinal);

    /// <summary>
    /// Every serializable type reachable from the signatures of
    /// <paramref name="grainInterfaces"/>: each method's parameter types and
    /// return type, unwrapped through tasks, arrays and generic arguments, then
    /// followed transitively through the serialized members of any
    /// <c>[GenerateSerializer]</c> type found, so a mutable buffer nested one or
    /// more levels below the boundary is still audited.
    /// </summary>
    private static IReadOnlyCollection<Type> ReachablePayloadTypes(IEnumerable<Type> grainInterfaces)
    {
        var visited = new HashSet<Type>();
        var queue = new Queue<Type>();

        foreach (var grainInterface in grainInterfaces)
        {
            foreach (var method in grainInterface.GetMethods(BindingFlags.Public | BindingFlags.Instance))
            {
                if (method.IsSpecialName) continue;

                Enqueue(method.ReturnType);
                foreach (var parameter in method.GetParameters()) Enqueue(parameter.ParameterType);
            }
        }

        while (queue.Count > 0)
        {
            var type = queue.Dequeue();
            if (!HasAttributeNamed(type, GenerateSerializerAttributeName)) continue;

            foreach (var member in SerializedMembers(type)) Enqueue(MemberType(member));
        }

        return visited;

        void Enqueue(Type type)
        {
            foreach (var candidate in Unwrap(type))
            {
                if (visited.Add(candidate)) queue.Enqueue(candidate);
            }
        }
    }

    /// <summary>
    /// Expands <paramref name="type"/> into the concrete payload types it can
    /// carry: itself, its element type when it is an array, and each generic
    /// argument (which covers <c>Task&lt;T&gt;</c>, <c>ValueTask&lt;T&gt;</c>,
    /// <c>IReadOnlyList&lt;T&gt;</c>, <c>Nullable&lt;T&gt;</c> and friends without
    /// naming any of them).
    /// </summary>
    private static IEnumerable<Type> Unwrap(Type type)
    {
        if (type is null || type == typeof(void) || type.IsPrimitive || type == typeof(string)) yield break;
        if (type.ContainsGenericParameters) yield break;

        yield return type;

        if (type.IsArray && type.GetElementType() is { } element)
        {
            foreach (var inner in Unwrap(element)) yield return inner;
        }

        if (type.IsGenericType)
        {
            foreach (var argument in type.GetGenericArguments())
            {
                foreach (var inner in Unwrap(argument)) yield return inner;
            }
        }
    }

    /// <summary>
    /// Descriptions of <paramref name="type"/>'s serialized members whose type is
    /// a mutable buffer - an array, or a non-<see cref="string"/> collection.
    /// Returned as <c>"Name (TypeName)"</c> strings so the failure message names
    /// the offending member rather than only the type.
    /// </summary>
    private static IEnumerable<string> MutableBufferMembers(Type type)
    {
        foreach (var member in SerializedMembers(type))
        {
            var memberType = MemberType(member);
            if (IsMutableBuffer(memberType))
            {
                yield return $"{member.Name} ({FriendlyName(memberType)})";
            }
        }
    }

    /// <summary>
    /// The <c>[Id]</c>-marked properties and fields of <paramref name="type"/>:
    /// the members Orleans actually serialises, which are also the ones a skipped
    /// deep copy would share.
    /// </summary>
    private static IEnumerable<MemberInfo> SerializedMembers(Type type)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        foreach (var property in type.GetProperties(flags))
        {
            if (HasAttributeNamed(property, IdAttributeName)) yield return property;
        }

        foreach (var field in type.GetFields(flags))
        {
            if (HasAttributeNamed(field, IdAttributeName)) yield return field;
        }
    }

    private static Type MemberType(MemberInfo member) =>
        member is PropertyInfo property ? property.PropertyType : ((FieldInfo)member).FieldType;

    /// <summary>
    /// Whether <paramref name="type"/> is a payload a receiver could write
    /// through: any array, or any type implementing <see cref="IEnumerable"/>
    /// other than <see cref="string"/>. An immutable-by-construction value
    /// (a primitive, an enum, a string, a <c>readonly record struct</c> of
    /// scalars) is not, and neither is anything from
    /// <c>System.Collections.Immutable</c> - an <c>ImmutableArray&lt;T&gt;</c>
    /// member offers no mutating operation, so sharing it across a skipped copy
    /// is exactly as safe as sharing an <see cref="int"/>.
    /// </summary>
    private static bool IsMutableBuffer(Type type)
    {
        if (type == typeof(string)) return false;
        if (type.IsArray) return true;

        var target = Nullable.GetUnderlyingType(type) ?? type;
        if (target == typeof(string)) return false;
        if (target.Namespace?.StartsWith("System.Collections.Immutable", StringComparison.Ordinal) == true) return false;

        return typeof(IEnumerable).IsAssignableFrom(target);
    }

    private static bool HasAttributeNamed(MemberInfo member, string attributeName) =>
        member.GetCustomAttributes(inherit: false).Any(a => a.GetType().Name == attributeName);

    /// <summary>A readable name for a generic type, e.g. <c>IReadOnlyList&lt;Byte[]&gt;</c>.</summary>
    private static string FriendlyName(Type type)
    {
        if (type.IsArray) return $"{FriendlyName(type.GetElementType()!)}[]";
        if (!type.IsGenericType) return type.Name;

        var name = type.Name;
        var tick = name.IndexOf('`');
        if (tick >= 0) name = name[..tick];
        return $"{name}<{string.Join(", ", type.GetGenericArguments().Select(FriendlyName))}>";
    }
}
