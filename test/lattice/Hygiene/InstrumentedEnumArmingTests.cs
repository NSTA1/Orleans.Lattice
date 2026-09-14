using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every member of an enum marked <see cref="InstrumentedEnumAttribute"/>
/// maps to a <b>distinct</b> armed value of its metric tag, so that an outcome the
/// code can produce can never be reported under another outcome's arm or under no
/// arm at all (issue #2939).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why distinctness rather than non-throwing.</b> The obvious gate - "the mapping
/// does not throw for any member" - is worthless on this repository, and provably so.
/// Two of the four marked enums map through a <c>switch</c> with a discard arm, and
/// both are <i>total today</i>, because the discard legitimately belongs to a real
/// member (<c>NotObserved</c>, <c>LeafWalk</c>). A non-throwing gate passes both, and
/// keeps passing on the day a further member is added and silently inherits that
/// discard. Distinctness reddens at exactly that moment: the new member collides with
/// the arm the discard already owns.
/// </para>
/// <para>
/// <b>Distinctness is not merely convenient, it is the correctly discriminating
/// predicate</b>, and the marked set contains an existence proof of each of the three
/// mapping shapes:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Throwing switch</b> (<c>ReactivationOutcome</c>). A member with no arm raises,
/// so the new member is caught at the first invocation. Distinctness subsumes this
/// because the invocation happens either way.
/// </description></item>
/// <item><description>
/// <b>Discard owned by a real member</b> (<c>ShardHealingDecision</c> to
/// <c>not_observed</c>, <c>ScanPagePhase</c> to <c>leaf_walk</c>). A new member is
/// reported under an existing member's arm and is <i>indistinguishable in the
/// exposition</i>. This is the silent case, it is the reason the gate exists, and
/// distinctness is what catches it.
/// </description></item>
/// <item><description>
/// <b>Discard is a dedicated sentinel no member claims</b>
/// (<c>WalGcCursorFloorState</c> to <c>unclassified</c>). A new member lands on an
/// arm that means "a state was added and nobody mapped it", which is visible to a
/// reader and is the behaviour that mapping's own remarks deliberately specify.
/// Distinctness stays green here, <b>correctly</b>: nothing is mis-attributed.
/// </description></item>
/// </list>
/// <para>
/// So the predicate reddens in exactly the unsafe shape and stays quiet in the safe
/// one. A blunter rule - "no discard arms in a tag mapping" - would have failed shape
/// 3, which is the best-behaved of the three, and would have pushed an author towards
/// shape 1 or 2 to satisfy the gate. That is the trap this fixture is meant to avoid
/// rather than set.
/// </para>
/// <para>
/// <b>The relation is one-directional and must stay that way.</b> This gate asserts
/// that every enum member has an arm. It asserts <i>nothing</i> about arms that have
/// no enum member, and a bidirectional version would be wrong rather than merely
/// stricter. On <c>orleans.lattice.wal.gc.blocked_leaf_reactivations</c> the tag
/// carries four terminal outcomes drawn from <c>ReactivationOutcome</c> alongside
/// <c>attempted</c>, <c>healed</c>, <c>abandoned</c> and <c>rearmed</c>, which are
/// lifecycle events and not enum members at all. A gate quantifying the other way
/// would fail on four legitimate arms on its first run, which is how this gate would
/// have become the ninth arity mismatch on this epic while being the gate written to
/// catch the eighth.
/// </para>
/// <para>
/// <b>What this gate is silent about.</b> Stated explicitly, because a gate whose
/// coverage is unstated will be assumed total.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>It does not prove the mapping is ever called.</b> An enum fully armed by a
/// mapping that no emission site invokes still produces no series.
/// <see cref="InstrumentEmissionCoverageTests"/> quantifies over declarations and is
/// the gate for that hazard; the two populations are disjoint in the failing case.
/// </description></item>
/// <item><description>
/// <b>It does not prove every member is reachable.</b> A member correctly armed but
/// never produced by any code path is indistinguishable here from one produced
/// constantly. That is the residual #2938 named and it needs a runtime witness, not
/// reflection.
/// </description></item>
/// <item><description>
/// <b>Scope is the core assembly.</b> <see cref="InstrumentedEnumAttribute"/> is
/// <see langword="internal"/>, so it can only mark enums in
/// <c>Orleans.Lattice</c>. That is a stated limit, not a defect, but the size of
/// what it leaves uncovered is <b>measured rather than recalled</b>: see
/// <see cref="Enums_outside_the_core_assembly_that_tag_a_metric_match_the_recorded_population"/>,
/// which scans <c>src/</c> and fails when that population drifts.
/// <para>
/// An earlier revision of this paragraph justified the limit by asserting that no
/// other package tagged a metric from an enum. That was false when it was written
/// (issue #2988), and the way it survived is the point: it was the one sentence in
/// a fixture whose whole doctrine is that its arguments are executed, that nothing
/// executed. A stale premise replaced by a fresher premise would expire the same
/// way, so the count is now computed and asserted instead of stated.
/// </para>
/// <see cref="Every_structurally_instrumented_enum_carries_the_marker"/> is
/// what stops the marker being under-applied <i>inside</i> that scope - and it
/// earned that role on its first run, catching a fourth instrumented enum that a
/// deliberate manual survey of this very question had missed.
/// </description></item>
/// <item><description>
/// <b>A shape-3 mapping is covered less strongly, by design.</b> A new member of
/// <c>WalGcCursorFloorState</c> lands on <c>unclassified</c> and this gate stays
/// green. That is the specified behaviour of that mapping, but it does mean the
/// gate's strength is not uniform across the marked set, and a reader should not
/// infer that every marked enum is held to the same standard.
/// </description></item>
/// <item><description>
/// <b>Duplicated <c>const string</c> switches are out of scope.</b>
/// <c>TombstoneCompactionGrain</c>'s trigger reasons and
/// <c>LatticeMetrics.PathWalk</c> are bare string constants, not enums, so there is
/// no member set to quantify over. They can drift and this gate will not say so.
/// </description></item>
/// </list>
/// <para>
/// <b>No visibility was widened for this gate, and none should be.</b> Issue #2939
/// anticipated that a general gate "would impose <see langword="internal"/> on every
/// marked enum". It does not. Reflection reads non-public types from
/// <see cref="Assembly.GetTypes"/> and invokes private statics through
/// <see cref="BindingFlags.NonPublic"/>, so two of the three mapping methods remain
/// <see langword="private"/> and are exercised here as-is.
/// </para>
/// </remarks>
[TestFixture]
[Category("Hygiene")]
public sealed class InstrumentedEnumArmingTests
{
    private const BindingFlags StaticMembers =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly;

    /// <summary>
    /// The enums known to carry the marker when this gate was written. Held as a
    /// <i>lower bound</i>, never as an equality: adding a fourth marked enum must not
    /// redden a gate whose whole purpose is to encourage marking. Removing one of
    /// these, however, is a regression and is meant to be loud.
    /// </summary>
    private static readonly string[] KnownMarkedEnums =
    [
        "ReactivationOutcome",
        "ShardHealingDecision",
        "ScanPagePhase",
        "WalGcCursorFloorState",
        "LeafStarvationDriveOutcome",
    ];

    private static readonly Lazy<IReadOnlyList<Type>> MarkedEnums = new(() =>
        typeof(LatticeMetrics).Assembly.GetTypes()
            .Where(static t => t.IsEnum && t.GetCustomAttribute<InstrumentedEnumAttribute>() is not null)
            .OrderBy(static t => t.FullName, StringComparer.Ordinal)
            .ToList());

    /// <summary>
    /// The load-bearing assertion. Every member of every marked enum maps, without
    /// throwing, onto the declared tag key and onto a value no other member claims.
    /// </summary>
    [Test]
    public void Every_marked_enum_member_maps_to_a_distinct_armed_tag()
    {
        var failures = new List<string>();

        foreach (var enumType in MarkedEnums.Value)
        {
            var marker = enumType.GetCustomAttribute<InstrumentedEnumAttribute>()!;
            var mapping = ResolveMapping(enumType, marker, failures);
            if (mapping is null)
            {
                continue;
            }

            var claimed = new Dictionary<string, string>(StringComparer.Ordinal);

            foreach (var member in Enum.GetValues(enumType).Cast<object>())
            {
                KeyValuePair<string, object?> tag;
                try
                {
                    tag = (KeyValuePair<string, object?>)mapping.Invoke(null, [member])!;
                }
                catch (TargetInvocationException ex)
                {
                    failures.Add(
                        $"{enumType.Name}.{member} has no armed tag: {mapping.Name} threw "
                        + $"{ex.InnerException?.GetType().Name}. Add an arm for it rather than "
                        + "letting it fall through.");
                    continue;
                }

                if (!string.Equals(tag.Key, marker.TagName, StringComparison.Ordinal))
                {
                    failures.Add(
                        $"{enumType.Name}.{member} maps to tag key '{tag.Key}' but its marker "
                        + $"declares '{marker.TagName}'. One of the two is wrong.");
                }

                var value = tag.Value as string ?? tag.Value?.ToString() ?? "<null>";
                if (claimed.TryGetValue(value, out var owner))
                {
                    failures.Add(
                        $"{enumType.Name}.{member} maps to tag value '{value}', which "
                        + $"{enumType.Name}.{owner} already claims. Two members reported under "
                        + "one arm are indistinguishable in the exposition, and this is the shape "
                        + "a new member takes when it silently inherits a discard arm.");
                    continue;
                }

                claimed[value] = member.ToString()!;
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// The marker names a real instrument. A marker field nothing verifies is the
    /// #2938 failure mode - prose that stays true-sounding while the code moves - so
    /// every one of the three arguments is checked somewhere in this fixture.
    /// </summary>
    [Test]
    public void Every_marked_enum_names_a_declared_instrument()
    {
        var declared = DeclaredInstrumentNames();
        Assert.That(
            declared,
            Is.Not.Empty,
            "No instruments were discovered in the core assembly, so the instrument-name "
            + "check below would pass vacuously for every marker.");

        var failures = MarkedEnums.Value
            .Select(static t => (Type: t, Marker: t.GetCustomAttribute<InstrumentedEnumAttribute>()!))
            .Where(x => !declared.Contains(x.Marker.InstrumentName))
            .Select(x =>
                $"{x.Type.Name} declares instrument '{x.Marker.InstrumentName}', which the core "
                + "assembly does not declare. Either the instrument was renamed or the marker is wrong.")
            .ToList();

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// The marker is not under-applied. Any static method in the core assembly that
    /// takes a single enum and returns a metric tag is, structurally, an instrumented
    /// enum's mapping - so that enum must carry the marker.
    /// <para>
    /// This is the arm that makes the generalisation self-maintaining. Without it the
    /// gate covers exactly the enums somebody remembered to mark, which is the
    /// hand-maintained list the marker was introduced to remove, wearing a marker.
    /// </para>
    /// </summary>
    [Test]
    public void Every_structurally_instrumented_enum_carries_the_marker()
    {
        var unmarked = StructurallyInstrumentedEnums()
            .Where(static x => x.Enum.GetCustomAttribute<InstrumentedEnumAttribute>() is null)
            .Select(static x =>
                $"{x.Enum.Name} is mapped to a metric tag by {x.Declaring.Name}.{x.Method.Name} "
                + "but carries no [InstrumentedEnum]. Mark it, so its members are held exhaustively "
                + "armed, or move the mapping if it is not a metric tag.")
            .ToList();

        Assert.That(unmarked, Is.Empty, string.Join(Environment.NewLine, unmarked));
    }

    /// <summary>
    /// The population of enums outside the core assembly that tag a metric, held as
    /// an exact set so that it cannot drift silently.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the executable replacement for a prose claim that was already false
    /// when it was written (issue #2988). It is deliberately an <b>equality</b> and
    /// not a lower bound, which is the opposite choice from
    /// <see cref="KnownMarkedEnums"/>, and the asymmetry is intended: marking more
    /// core enums is the behaviour this fixture wants to encourage, so a lower bound
    /// is right there. Here, a <i>new</i> unguarded enum outside core is exactly the
    /// drift the sentence failed to notice, so growth must be as loud as shrinkage.
    /// </para>
    /// <para>
    /// Recorded as (package, mapping method, enum) rather than as a bare count. A
    /// count would redden on drift but would not say what moved, and a reader would
    /// then have to re-derive the population by hand - which is how the original
    /// claim came to be trusted in the first place.
    /// </para>
    /// </remarks>
    private static readonly string[] RecordedOutsideCoreTagMappings =
    [
        "lattice.api.mcp.repocontext Tag(RepoContextMemoryRestoreOutcome)",
        "lattice.backup KindTag(BackupKind)",
    ];

    /// <summary>
    /// Every enum outside the core assembly that is mapped to a metric tag is
    /// accounted for. These enums are <b>not</b> covered by any other arm of this
    /// fixture: the marker is <see langword="internal"/> to <c>Orleans.Lattice</c>,
    /// so they cannot carry it, and reflection here cannot see them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Source scanning rather than reflection is a deliberate choice, not a
    /// convenience. <c>Orleans.Lattice.Tests</c> does not reference every package
    /// that declares an instrument - it references <c>Orleans.Lattice.Backup</c> but
    /// not <c>Orleans.Lattice.Api.Mcp.RepoContext</c> - so a reflection-based census
    /// would silently under-report exactly the packages it failed to reference, and
    /// would do so in the same shape as the defect this assertion exists to prevent.
    /// A source scan is independent of the assembly graph.
    /// </para>
    /// <para>
    /// <b>What this does not claim.</b> It quantifies over the same structural shape
    /// the rest of this fixture uses - a static method taking one enum and returning
    /// a tag. An enum reported as a metric tag through <c>const string</c> values at
    /// the emission site has no such method and is not counted here, so this
    /// population is a lower bound on "enums that influence a tag" while being exact
    /// for "enums with a tag-mapping method". That gap is real and is recorded in
    /// issue #2988 rather than papered over.
    /// </para>
    /// </remarks>
    [Test]
    public void Enums_outside_the_core_assembly_that_tag_a_metric_match_the_recorded_population()
    {
        var found = SourceTagMappings(insideCore: false)
            .OrderBy(static m => m, StringComparer.Ordinal)
            .ToList();

        var recorded = RecordedOutsideCoreTagMappings
            .OrderBy(static m => m, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            found,
            Is.EqualTo(recorded),
            "The set of enums outside the core assembly that map to a metric tag has changed.\n"
            + $"found:    {string.Join(" | ", found)}\n"
            + $"recorded: {string.Join(" | ", recorded)}\n"
            + "If an entry was added, that enum's members are NOT held exhaustively armed by any "
            + "gate - the marker is internal to Orleans.Lattice and cannot be applied from another "
            + "package without widening this fixture's three assembly bindings and its "
            + "core-only instrument-name check (issue #2988). Record it here deliberately, so the "
            + "gap stays counted rather than becoming folklore.");
    }

    /// <summary>
    /// Anti-vacuity for the source scan. The same pattern, applied to the core
    /// assembly's sources, must find the mappings reflection already proves are
    /// there.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This control is not load-bearing today, and saying so is the point.</b>
    /// Measured: breaking the scan pattern reddens
    /// <see cref="Enums_outside_the_core_assembly_that_tag_a_metric_match_the_recorded_population"/>
    /// as well, because the recorded set is currently non-empty, so an empty scan
    /// fails that equality on its own.
    /// </para>
    /// <para>
    /// It becomes the only guard at the moment the outside-core population is driven
    /// to <b>zero</b> - which is the intended end state of issue #2988. A recorded
    /// set of zero compared against a scan that matches nothing is a constant
    /// compared against itself, and it would report a clean repository no matter what
    /// the source said. Holding the pattern to a population reflection enumerates
    /// from metadata, rather than from text, is what keeps that emptiness a
    /// measurement instead of an artefact.
    /// </para>
    /// </remarks>
    [Test]
    public void The_source_scan_pattern_finds_the_core_mappings_reflection_already_proves()
    {
        var scanned = SourceTagMappings(insideCore: true)
            .Select(static m => m[(m.IndexOf('(') + 1)..].TrimEnd(')'))
            .ToList();

        Assert.That(
            scanned,
            Is.SupersetOf(MarkedEnums.Value.Select(static t => t.Name)),
            "The source scan did not find every enum that reflection reports as marked in the core "
            + "assembly, so the pattern does not match this repository's code and any outside-core "
            + "result it produces is meaningless.\n"
            + $"scanned: {string.Join(", ", scanned.OrderBy(static s => s, StringComparer.Ordinal))}");
    }

    /// <summary>
    /// Scans <c>src/</c> for static methods that take a single enum and return a
    /// metric tag, returning <c>"package Method(EnumName)"</c> for each.
    /// </summary>
    /// <param name="insideCore">
    /// When <see langword="true"/>, restricts the scan to <c>src/lattice/</c>, the
    /// core assembly; when <see langword="false"/>, excludes it.
    /// </param>
    private static IReadOnlyList<string> SourceTagMappings(bool insideCore)
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");
        var files = HygieneRepository.EnumerateFiles(src, "*.cs").ToList();

        var enumNames = new HashSet<string>(StringComparer.Ordinal);
        var texts = new List<(string Package, string Text)>();

        foreach (var file in files)
        {
            var relative = Path.GetRelativePath(src, file).Replace('\\', '/');
            var package = relative.Split('/')[0];
            if (package.Equals("lattice", StringComparison.Ordinal) != insideCore)
            {
                continue;
            }

            var text = File.ReadAllText(file);
            texts.Add((package, text));

            foreach (Match m in EnumDeclaration.Matches(text))
            {
                enumNames.Add(m.Groups[1].Value);
            }
        }

        var mappings = new List<string>();

        foreach (var (package, text) in texts)
        {
            foreach (Match m in TagMappingMethod.Matches(text))
            {
                var parameter = m.Groups[2].Value;
                var simple = parameter[(parameter.LastIndexOf('.') + 1)..];
                if (enumNames.Contains(simple))
                {
                    mappings.Add($"{package} {m.Groups[1].Value}({simple})");
                }
            }
        }

        return mappings.Distinct(StringComparer.Ordinal).ToList();
    }

    private static readonly Regex EnumDeclaration = new(
        @"\benum\s+(\w+)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex TagMappingMethod = new(
        @"static\s+KeyValuePair<string,\s*object\?>\s+(\w+)\s*\(\s*([\w\.]+)\s+",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Anti-vacuity. A repository-wide gate that silently matches nothing reports
    /// green against any source at all, which is worse than having no gate, because
    /// it also reports that the property is held.
    /// </summary>
    [Test]
    public void Gate_inputs_are_not_vacuous()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                MarkedEnums.Value,
                Is.Not.Empty,
                "No enum in the core assembly carries [InstrumentedEnum]. Every assertion in this "
                + "fixture quantifies over that set and would pass against any source at all.");

            Assert.That(
                StructurallyInstrumentedEnums(),
                Is.Not.Empty,
                "No static method in the core assembly maps an enum to a KeyValuePair<string, object?>. "
                + "The under-application arm quantifies over that scan and would pass vacuously.");

            Assert.That(
                MarkedEnums.Value.Select(static t => t.Name).ToList(),
                Is.SupersetOf(KnownMarkedEnums),
                "An enum that carried the marker when this gate was written no longer does. Removing a "
                + "marker removes this gate's coverage of that enum silently, so it is asserted rather "
                + "than trusted. Adding new marked enums is expected and does not fail this check.");
        });
    }

    /// <summary>
    /// Per-enum anti-vacuity. Every marked enum resolves, declares at least one
    /// member, and yields at least one armed tag value, so no individual enum is
    /// carried through the distinctness check over an empty set.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="Gate_inputs_are_not_vacuous"/> guards the <i>population</i>: it
    /// fails when no enum carries the marker at all. It says nothing about an
    /// individual member of that population, and the two are not the same property.
    /// An empty enum is legal C# - <c>enum E { }</c> compiles - so a marked enum with
    /// no members walks the whole of
    /// <see cref="Every_marked_enum_member_maps_to_a_distinct_armed_tag"/> without
    /// entering the loop body once. It is green, it is counted as covered, and it
    /// asserts nothing, while the population check remains satisfied by its
    /// siblings.
    /// </para>
    /// <para>
    /// That is the arity defect one scale down, and it is exactly what issue #2944
    /// asks for in its points 2 and 3: assert the member set and the armed set are
    /// <b>non-zero before comparing</b>, rather than after. The distinction the epic
    /// keeps arriving at is that <b>the denominator has to be read, not the
    /// verdict</b> - a gate reporting "all members armed" over zero members is the
    /// same statement as an unprimed counter reporting a zero.
    /// </para>
    /// <para>
    /// The armed-set clause is the stronger half. A mapping that threw for every
    /// member would leave the member count non-zero and the armed set empty, and the
    /// distinctness check would report those throws - but it would report them as
    /// individual failures, and a future refactor that swallowed them would leave
    /// this shape undetected. Asserting the armed set is non-empty pins the property
    /// directly rather than relying on another clause's diagnostics.
    /// </para>
    /// </remarks>
    [Test]
    public void Every_marked_enum_has_members_and_arms_before_it_is_compared()
    {
        var failures = new List<string>();

        foreach (var enumType in MarkedEnums.Value)
        {
            var members = Enum.GetValues(enumType).Cast<object>().ToList();
            if (members.Count == 0)
            {
                failures.Add(
                    $"{enumType.Name} carries [InstrumentedEnum] but declares no members, so the "
                    + "exhaustive-arming check walks an empty set for it and passes without asserting "
                    + "anything. Remove the marker or give the enum its members.");
                continue;
            }

            var marker = enumType.GetCustomAttribute<InstrumentedEnumAttribute>()!;
            var probe = new List<string>();
            var mapping = ResolveMapping(enumType, marker, probe);
            if (mapping is null)
            {
                failures.AddRange(probe);
                continue;
            }

            var armed = new HashSet<string>(StringComparer.Ordinal);
            foreach (var member in members)
            {
                try
                {
                    var tag = (KeyValuePair<string, object?>)mapping.Invoke(null, [member])!;
                    if (tag.Value?.ToString() is { Length: > 0 } value)
                    {
                        armed.Add(value);
                    }
                }
                catch (TargetInvocationException)
                {
                    // Reported in detail by the distinctness arm; here only its
                    // contribution to the armed set matters, which is none.
                }
            }

            if (armed.Count == 0)
            {
                failures.Add(
                    $"{enumType.Name} has {members.Count} member(s) but yields no armed tag value at all "
                    + $"through {mapping.Name}. Every member is unarmed, so the instrument carries no "
                    + "series for this enum and an absent series reads as an outcome that never occurred.");
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// Resolves an enum's tag mapping by <i>signature</i> rather than by name: the
    /// unique static method on the declaring type taking the enum and returning a tag.
    /// Matching on a name would put a stringly-typed member reference in the marker
    /// that nothing keeps current; matching on the signature makes both absence and
    /// ambiguity loud.
    /// </summary>
    private static MethodInfo? ResolveMapping(
        Type enumType,
        InstrumentedEnumAttribute marker,
        List<string> failures)
    {
        var candidates = MappingMethods(marker.TagMappingDeclaringType)
            .Where(m => m.GetParameters()[0].ParameterType == enumType)
            .ToList();

        switch (candidates.Count)
        {
            case 1:
                return candidates[0];

            case 0:
                failures.Add(
                    $"{enumType.Name} declares its mapping on {marker.TagMappingDeclaringType.Name}, "
                    + "which has no static method taking that enum and returning "
                    + "KeyValuePair<string, object?>.");
                return null;

            default:
                failures.Add(
                    $"{enumType.Name} resolves {candidates.Count} tag mappings on "
                    + $"{marker.TagMappingDeclaringType.Name} ({string.Join(", ", candidates.Select(static c => c.Name))}). "
                    + "The mapping is resolved by signature, so it must be unique.");
                return null;
        }
    }

    private static IEnumerable<MethodInfo> MappingMethods(Type declaringType) =>
        declaringType.GetMethods(StaticMembers)
            .Where(static m => m.ReturnType == typeof(KeyValuePair<string, object?>))
            .Where(static m => m.GetParameters() is [{ ParameterType.IsEnum: true }]);

    /// <summary>
    /// Every enum the core assembly maps to a metric tag, discovered from method
    /// signatures alone and therefore independent of whether anyone marked it.
    /// </summary>
    private static IReadOnlyList<(Type Enum, Type Declaring, MethodInfo Method)> StructurallyInstrumentedEnums() =>
        typeof(LatticeMetrics).Assembly.GetTypes()
            .SelectMany(static t => MappingMethods(t).Select(m => (Enum: m.GetParameters()[0].ParameterType, Declaring: t, Method: m)))
            .OrderBy(static x => x.Enum.FullName, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Instrument names declared anywhere in the core assembly, read from the live
    /// <see cref="Instrument"/> objects rather than from source, so a renamed
    /// instrument cannot satisfy a marker that still names the old string.
    /// </summary>
    private static IReadOnlySet<string> DeclaredInstrumentNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var type in typeof(LatticeMetrics).Assembly.GetTypes())
        {
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            {
                if (!typeof(Instrument).IsAssignableFrom(field.FieldType))
                {
                    continue;
                }

                Instrument? instrument;
                try
                {
                    instrument = field.GetValue(null) as Instrument;
                }
                catch (Exception)
                {
                    // A field whose static initialiser needs a host is not evidence either way.
                    continue;
                }

                if (instrument is not null)
                {
                    names.Add(instrument.Name);
                }
            }
        }

        return names;
    }
}
