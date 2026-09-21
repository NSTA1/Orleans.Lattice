using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: every <c>[TestFixture]</c> that exercises an out-of-process
/// dependency - an Orleans <c>Orleans.TestingHost.TestCluster</c>, an
/// ASP.NET Core <c>TestServer</c>, a <c>Microsoft.Extensions.Hosting.IHost</c>,
/// a <c>Grpc.Net.Client.GrpcChannel</c>, or any user-defined
/// <c>*ClusterFixture</c> helper - must carry one of the slow-category tags
/// (<c>Integration</c>, <c>Chaos</c>, or <c>AzureStorageEmulator</c>) at the
/// fixture level so it is excluded from the Tier 2 fast dev loop, or must
/// declare an explicit, measured <see cref="FastInProcessHostFixtureAttribute"/>
/// exemption.
/// <para>
/// Without the tag, a single such fixture re-introduces silo startup latency
/// into the inner dev loop and silently inflates Tier 2 from seconds to
/// minutes.
/// </para>
/// <para>
/// WHAT COUNTS AS A SIGNAL, AND WHY. Detection is by <b>type</b>, never by
/// name: a unit test that happens to be <c>*IntegrationTests</c> by name must
/// not be flagged. That principle is unchanged. What changed in issue #3142 is
/// the set of places a type is looked for. Restricting the search to declared
/// fields and properties discarded every host that is only ever a
/// <b>local</b> - a <c>WebApplication</c> built, started and disposed inside a
/// helper method - and a source census found that every untagged host-building
/// fixture in the repository was of exactly that shape: 10 of 10 misses were
/// method-body construction, so the gate was silent on precisely the case it
/// was written to catch. The scan therefore now covers
/// </para>
/// <list type="bullet">
///   <item><description>declared field and property types (the original pass),</description></item>
///   <item><description>method-body local variable types, via
///     <see cref="MethodBody.LocalVariables"/>, which catches a host held in a
///     synchronous local, and</description></item>
///   <item><description>the fields of compiler-generated nested types - async
///     state machines and lambda display classes - because a local that is live
///     across an <c>await</c> or captured by a closure is lowered out of the
///     method body and into a field of one of those types, where the pass above
///     can no longer see it, and</description></item>
///   <item><description>call sites in the IL itself, because a host that is
///     constructed and handed straight on - <c>return Wrap(GrpcChannel.ForAddress(a))</c> -
///     never gets a local slot at all, so the compiler leaves no trace of it in
///     any of the passes above. This one is measured, not assumed:
///     <c>StaticCredentialTransportGateTests</c> is exactly that shape and is
///     invisible to all three.</description></item>
/// </list>
/// <para>
/// A type matches if its own full name is in the bearing set, or if any of its
/// base types or implemented interfaces is. The transitive walk is load-bearing
/// rather than defensive: the single commonest shape in this repository is
/// <c>await using var app = builder.Build();</c>, whose local is a
/// <c>Microsoft.AspNetCore.Builder.WebApplication</c>. That name is not in the
/// set and never will be - it is matched because <c>WebApplication</c>
/// implements <c>IHost</c>, which is.
/// </para>
/// <para>
/// Deliberately NOT matched: generic type arguments. Unwrapping them would
/// catch a <c>Task&lt;IHost&gt;</c>, but it would equally catch a
/// <c>Mock&lt;IHost&gt;</c> in a pure unit test, which builds no host at all.
/// The awaiter and result fields a state machine holds are redundant with the
/// hoisted local itself, so the recall given up here is nil and the
/// false-positive surface avoided is real.
/// </para>
/// <para>
/// The scan targets <see cref="object.GetType"/>'s assembly, so a concrete
/// subclass in each test project runs the gate against that project's own
/// assembly. See <c>.github/instructions/testing.instructions.md</c> section
/// "Categorization conventions".
/// </para>
/// </summary>
public abstract class IntegrationCategoryHygieneTestsBase
{
    // NUnit categories that exclude a fixture from the Tier 2 fast dev loop.
    // A cluster-based fixture must carry at least one of these so it stays
    // out of the fast loop.
    private static readonly HashSet<string> SlowCategories = new(StringComparer.Ordinal)
    {
        "Integration",
        "Chaos",
        "AzureStorageEmulator",
    };

    // Full type names whose presence on a fixture - as a field, a property, a
    // method-body local, or a compiler-hoisted capture - indicates the fixture
    // spins up an out-of-process or host-level dependency. Matched by FullName
    // so this test does not require a compile-time reference to every assembly
    // listed here - the type only needs to be loaded in the test AppDomain,
    // which it will be if some fixture in the assembly uses it.
    private static readonly HashSet<string> IntegrationBearingTypeNames = new(StringComparer.Ordinal)
    {
        "Orleans.TestingHost.TestCluster",
        "Microsoft.AspNetCore.TestHost.TestServer",
        "Microsoft.Extensions.Hosting.IHost",
        "Grpc.Net.Client.GrpcChannel",
    };

    private const BindingFlags DeclaredMembers =
        BindingFlags.Instance | BindingFlags.Static
        | BindingFlags.Public | BindingFlags.NonPublic
        | BindingFlags.DeclaredOnly;

    // Bound on the nested-type walk. A compiler-generated state machine or
    // display class sits one level below the method's declaring type, and a
    // closure inside an async method's lambda one level below that; three is
    // slack over the deepest shape the compiler emits, and stops a pathological
    // nesting chain from turning the gate into an unbounded walk.
    private const int MaxNestedDepth = 3;

    /// <summary>
    /// Walks every loaded <c>[TestFixture]</c> in the consuming test
    /// assembly, decides whether it is a cluster-based fixture using only
    /// type signals, and fails if a detected fixture neither carries a
    /// slow-category tag nor declares a measured
    /// <see cref="FastInProcessHostFixtureAttribute"/> exemption. The failure
    /// message lists every offending fixture's <see cref="Type.FullName"/>,
    /// which signal detected it, and the categories it already carries, so the
    /// fix is mechanical.
    /// </summary>
    [Test]
    public void Every_cluster_based_fixture_carries_a_slow_category()
    {
        var assembly = GetType().Assembly;

        var allTypes = SafeGetTypes(assembly).ToList();

        var fixtures = allTypes
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(HasTestFixtureAttribute)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        // Anti-vacuity control (issue #2275), in three parts, because the
        // obvious one is worthless here.
        //
        // NOT ASSERTED: fixtures.Count > 0. This fixture is itself a
        // [TestFixture] in the assembly it reflects over, so that predicate is
        // TRUE BY CONSTRUCTION and could not have come out differently. It
        // would read exactly like a control while proving nothing - the defect
        // this whole control exists to prevent.
        //
        // (1) The real vacuity path is SafeGetTypes degrading: it swallows
        // ReflectionTypeLoadException and returns only the types that did
        // load, so a load failure shrinks the population silently and the gate
        // reports clean over whatever survived.
        HygieneDenominator.RequireExamined(
            allTypes.Count, nameof(IntegrationCategoryHygieneTestsBase), "loaded types", assembly.FullName ?? assembly.ToString());

        // (2) End-to-end self-detection: the running fixture must find ITSELF
        // through the same reflection and attribute-reading path it uses to
        // find everything else. Unlike a count, this can genuinely fail - if
        // HasTestFixtureAttribute or the type walk breaks, the gate stops
        // detecting fixtures and this is what says so.
        Assert.That(fixtures, Does.Contain(GetType()),
            $"HYGIENE GATE VACUOUS: '{nameof(IntegrationCategoryHygieneTestsBase)}' did not detect its own "
            + $"fixture type '{GetType().FullName}' while reflecting over {assembly.GetName().Name}, so its "
            + "fixture-discovery path is broken and its clean report means nothing. "
            + $"It found {fixtures.Count} fixture(s) among {allTypes.Count} loaded type(s).");

        // (3) End-to-end self-detection for the two method-body passes added
        // in #3142, for the same reason and with the same shape as (2). A
        // count cannot serve here either: an assembly whose fixtures happen to
        // declare no bearing local is legitimately zero, so a denominator on
        // locals examined would fire on healthy assemblies and be deleted. The
        // probes below instead exercise the exact mechanism against a sentinel
        // type of their own, so a GetMethodBody path that starts returning null
        // - or a nested-type walk that stops finding state machines - fails
        // here rather than quietly reporting the repository clean.
        AssertMethodBodyPassesAreLive();

        var violations = new List<string>();
        foreach (var fixture in fixtures)
        {
            var signal = DescribeClusterSignal(fixture, IntegrationBearingTypeNames);
            var exemption = fixture.GetCustomAttribute<FastInProcessHostFixtureAttribute>(inherit: false);
            var categories = GetCategoryNames(fixture);
            var isSlow = categories.Overlaps(SlowCategories);

            if (signal is null)
            {
                // A stale exemption is a violation in its own right. Left
                // alone it would read, forever, as a live measured decision
                // about a fixture that no longer builds a host at all.
                if (exemption is not null)
                {
                    violations.Add($"{fixture.FullName}: carries [FastInProcessHostFixture] but builds no host "
                        + "that this gate can detect, so the exemption is stale. Remove the attribute.");
                }

                continue;
            }

            if (exemption is not null)
            {
                if (isSlow)
                {
                    violations.Add($"{fixture.FullName}: carries BOTH a slow category "
                        + $"[{Render(categories)}] and a [FastInProcessHostFixture] exemption, which "
                        + "contradict each other. Keep the category and delete the exemption, or vice versa.");
                }
                else if (!FastInProcessHostFixtureAttribute.IsSubstantiveJustification(exemption.Justification))
                {
                    violations.Add($"{fixture.FullName}: [FastInProcessHostFixture] justification is not "
                        + $"substantive. It must be at least {FastInProcessHostFixtureAttribute.MinimumJustificationLength} "
                        + "characters and contain a digit, because the only defensible reason to keep a "
                        + "host-building fixture in the fast loop is a measurement, and a measurement has a "
                        + $"number in it. Got: \"{exemption.Justification}\".");
                }

                continue;
            }

            if (!isSlow)
            {
                violations.Add($"{fixture.FullName}: cluster-based fixture (detected via {signal}) "
                    + $"is missing a slow-category tag. Existing categories: [{Render(categories)}].");
            }
        }

        Assert.That(violations, Is.Empty,
            "Cluster-based test fixtures must declare a slow-category tag at the fixture level "
            + "so they are excluded from the Tier 2 dev-loop filter. Add "
            + "[Category(\"Integration\")] (or [Category(\"Chaos\")] for stress suites, or "
            + "[Category(\"AzureStorageEmulator\")] for emulator-dependent suites) above the "
            + "[TestFixture] declaration. Where the fixture builds only an in-memory host and has "
            + "been MEASURED cheap, declare [FastInProcessHostFixture(\"...\")] with that "
            + "measurement instead. See .github/instructions/testing.instructions.md "
            + "section 'Categorization conventions'."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static string Render(HashSet<string> categories) =>
        categories.Count == 0 ? "<none>" : string.Join(", ", categories.OrderBy(c => c, StringComparer.Ordinal));

    /// <summary>
    /// Returns a short description of the signal that identified
    /// <paramref name="fixture"/> as cluster-based, or <see langword="null"/>
    /// when no signal fired. The description is carried into the failure
    /// message so a reader can tell a declared member from a hoisted local
    /// without re-running the scan by hand.
    /// <para>
    /// This is the detector's public seam. The gate itself calls it with
    /// <c>IntegrationBearingTypeNames</c>; a test can call it with a set of
    /// its own sentinel types to exercise one lowering in isolation, without
    /// depending on what the assembly under scan happens to contain. It is
    /// public precisely so that such a test does not have to reflect past the
    /// seam - a reflected call would prove the method works when invoked and
    /// nothing about whether the gate invokes it.
    /// </para>
    /// </summary>
    /// <param name="fixture">The fixture type to inspect.</param>
    /// <param name="bearingNames">
    /// Full names of the types whose construction counts as a signal.
    /// </param>
    public static string? DescribeClusterSignal(Type fixture, IReadOnlySet<string> bearingNames)
    {
        // One memo per detection run. Interface and base-type walks resolve
        // the same handful of framework types over and over across a fixture's
        // methods, so memoising the verdict keeps the pass to one walk per
        // distinct type rather than one per occurrence.
        var memo = new Dictionary<Type, bool>();

        for (var t = fixture; t is not null && t != typeof(object); t = t.BaseType)
        {
            if (DeclaresBearingMember(t, bearingNames, memo)) return "field/property type";
            if (DeclaresBearingLocal(t, bearingNames, memo)) return "method-body local";
            if (NestedTypeBears(t, bearingNames, memo, depth: 0))
                return "compiler-generated capture (async state machine or lambda closure)";
            if (CallsBearingMemberDeep(t, bearingNames, memo, depth: 0)) return "IL call site";
        }

        return null;
    }

    private static bool DeclaresBearingMember(Type type, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        foreach (var f in type.GetFields(DeclaredMembers))
            if (IsBearing(f.FieldType, bearingNames, memo)) return true;

        foreach (var p in type.GetProperties(DeclaredMembers))
            if (IsBearing(p.PropertyType, bearingNames, memo)) return true;

        return false;
    }

    /// <summary>
    /// True when any method or constructor declared on <paramref name="type"/>
    /// has a local variable of a bearing type. This is the pass that sees a
    /// host which is built, used and disposed entirely inside a method body
    /// and never stored on the fixture.
    /// </summary>
    private static bool DeclaresBearingLocal(Type type, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        MethodBase[] methods;
        MethodBase[] constructors;
        try
        {
            methods = type.GetMethods(DeclaredMembers);
            constructors = type.GetConstructors(DeclaredMembers);
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            return false;
        }

        return AnyBearingLocal(methods, bearingNames, memo)
            || AnyBearingLocal(constructors, bearingNames, memo);
    }

    private static bool AnyBearingLocal(MethodBase[] methods, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        for (var i = 0; i < methods.Length; i++)
        {
            MethodBody? body;
            try
            {
                // Abstract, extern and runtime-provided methods have no body
                // and return null; a generic definition whose constraints
                // cannot be loaded throws. Neither is a signal.
                body = methods[i].GetMethodBody();
            }
            catch (Exception ex) when (IsMetadataFault(ex))
            {
                continue;
            }

            if (body is null) continue;

            var locals = body.LocalVariables;
            for (var j = 0; j < locals.Count; j++)
            {
                Type localType;
                try
                {
                    localType = locals[j].LocalType;
                }
                catch (Exception ex) when (IsMetadataFault(ex))
                {
                    continue;
                }

                if (IsBearing(localType, bearingNames, memo)) return true;
            }
        }

        return false;
    }

    /// <summary>
    /// True when a nested type of <paramref name="type"/> - in practice an
    /// async state machine or a lambda display class - holds a bearing type in
    /// a field or a local. A local that is live across an <c>await</c>, or
    /// captured by a closure, is lowered by the compiler out of the method
    /// body and into a field here, where <see cref="DeclaresBearingLocal"/>
    /// can no longer reach it.
    /// </summary>
    private static bool NestedTypeBears(Type type, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo, int depth)
    {
        if (depth >= MaxNestedDepth) return false;

        Type[] nested;
        try
        {
            nested = type.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic);
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            return false;
        }

        for (var i = 0; i < nested.Length; i++)
        {
            var candidate = nested[i];
            if (DeclaresBearingMember(candidate, bearingNames, memo)) return true;
            if (DeclaresBearingLocal(candidate, bearingNames, memo)) return true;
            if (NestedTypeBears(candidate, bearingNames, memo, depth + 1)) return true;
        }

        return false;
    }

    /// <summary>
    /// True when <paramref name="type"/>, or any of its nested types, contains
    /// a call or object construction whose target involves a bearing type.
    /// <para>
    /// This is the last and most expensive pass, and it exists because the
    /// three cheaper ones share a blind spot: they all look for a bearing type
    /// in a <b>slot</b>, and a value that is constructed and consumed in one
    /// expression never occupies one. Roslyn emits
    /// <c>call GrpcChannel::ForAddress; call CreateCallInvoker</c> with no
    /// local at all, so the channel is present in the program and absent from
    /// every field, property, local and capture. Reading the call sites is the
    /// only place that value is still visible.
    /// </para>
    /// <para>
    /// It runs only after the cheaper passes have all declined, so the common
    /// fixture never reaches it.
    /// </para>
    /// </summary>
    private static bool CallsBearingMemberDeep(Type type, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo, int depth)
    {
        if (depth >= MaxNestedDepth) return false;

        MethodBase[] methods;
        MethodBase[] constructors;
        Type[] nested;
        try
        {
            methods = type.GetMethods(DeclaredMembers);
            constructors = type.GetConstructors(DeclaredMembers);
            nested = type.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic);
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            return false;
        }

        if (AnyBearingCallSite(methods, bearingNames, memo)) return true;
        if (AnyBearingCallSite(constructors, bearingNames, memo)) return true;

        for (var i = 0; i < nested.Length; i++)
            if (CallsBearingMemberDeep(nested[i], bearingNames, memo, depth + 1)) return true;

        return false;
    }

    private static bool AnyBearingCallSite(MethodBase[] methods, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        for (var i = 0; i < methods.Length; i++)
        {
            var method = methods[i];

            byte[]? il;
            try
            {
                il = method.GetMethodBody()?.GetILAsByteArray();
            }
            catch (Exception ex) when (IsMetadataFault(ex))
            {
                continue;
            }

            if (il is null || il.Length == 0) continue;
            if (ScanIlForBearingToken(method, il, bearingNames, memo)) return true;
        }

        return false;
    }

    /// <summary>
    /// Walks an IL stream and resolves every member token it carries, testing
    /// each resolved member's declaring type and return type against the
    /// bearing set.
    /// <para>
    /// The walk is a real instruction walk, not a search for opcode bytes. An
    /// operand can contain any byte, so scanning for <c>0x28</c> and reading
    /// the next four bytes as a token would manufacture call sites out of
    /// string offsets and branch targets - and a false positive here fails a
    /// gate on an innocent fixture, which is the expensive direction to be
    /// wrong in. Stepping instruction by instruction means every token read is
    /// genuinely an operand.
    /// </para>
    /// <para>
    /// The opcode table is built by reflecting over <see cref="OpCodes"/>
    /// rather than hardcoded, so it cannot drift from the runtime's own
    /// definition. An opcode the table does not know stops the walk rather
    /// than guessing an operand width, because a wrong width desynchronises
    /// the stream and every token after it would be garbage.
    /// </para>
    /// <para>
    /// Only <see cref="OperandType.InlineMethod"/> operands are inspected -
    /// the operand of <c>call</c>, <c>callvirt</c>, <c>newobj</c> and
    /// <c>ldftn</c>. <see cref="OperandType.InlineTok"/> is deliberately
    /// excluded, and that exclusion was forced by a measured false positive:
    /// <c>DocsSnippetCompilationTestsBase</c> writes <c>typeof(IHost)</c> to
    /// locate the ASP.NET shared framework for its metadata reference set,
    /// which emits <c>ldtoken IHost</c>. Reading that token flagged a Roslyn
    /// compilation fixture as a cluster fixture. A <c>ldtoken</c> names a type
    /// without ever instantiating one, so it is not evidence of a host and is
    /// not read.
    /// </para>
    /// </summary>
    private static bool ScanIlForBearingToken(MethodBase method, byte[] il, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        var table = OpCodeTable.Value;
        var module = method.Module;

        Type[]? typeArguments = null;
        Type[]? methodArguments = null;
        try
        {
            if (method.DeclaringType is { IsGenericType: true } declaring)
                typeArguments = declaring.GetGenericArguments();
            if (method.IsGenericMethod)
                methodArguments = method.GetGenericArguments();
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            return false;
        }

        var position = 0;
        while (position < il.Length)
        {
            short code = il[position++];
            if (code == 0xFE)
            {
                if (position >= il.Length) break;
                code = (short)(0xFE00 | il[position++]);
            }

            if (!table.TryGetValue(code, out var opCode)) break;

            var operandSize = OperandSize(opCode.OperandType, il, position);
            if (operandSize < 0 || position + operandSize > il.Length) break;

            if (opCode.OperandType is OperandType.InlineMethod)
            {
                var token = BitConverter.ToInt32(il, position);
                if (ResolvedTokenIsBearing(module, token, typeArguments, methodArguments, bearingNames, memo))
                    return true;
            }

            position += operandSize;
        }

        return false;
    }

    private static int OperandSize(OperandType operandType, byte[] il, int position) => operandType switch
    {
        OperandType.InlineNone => 0,
        OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
        OperandType.InlineVar => 2,
        OperandType.InlineBrTarget or OperandType.InlineField or OperandType.InlineI
            or OperandType.InlineMethod or OperandType.InlineSig or OperandType.InlineString
            or OperandType.InlineTok or OperandType.InlineType or OperandType.ShortInlineR => 4,
        OperandType.InlineI8 or OperandType.InlineR => 8,
        OperandType.InlineSwitch => position + 4 <= il.Length
            ? 4 + (BitConverter.ToInt32(il, position) * 4)
            : -1,
        _ => -1,
    };

    private static bool ResolvedTokenIsBearing(
        Module module,
        int token,
        Type[]? typeArguments,
        Type[]? methodArguments,
        IReadOnlySet<string> bearingNames,
        Dictionary<Type, bool> memo)
    {
        MemberInfo? member;

        // A token resolves the same way every time within a module, and the
        // same handful of framework members are referenced from thousands of
        // call sites, so the resolve - which is the whole cost of this pass -
        // is done once each. Only the non-generic context is cached, because
        // that is the one whose result depends on nothing but the key.
        var cacheable = typeArguments is null && methodArguments is null;
        var key = (module, token);

        if (cacheable)
        {
            if (!ResolvedTokens.TryGetValue(key, out member))
            {
                member = ResolveMemberOrNull(module, token, null, null);
                ResolvedTokens[key] = member;
            }
        }
        else
        {
            member = ResolveMemberOrNull(module, token, typeArguments, methodArguments);
        }

        if (member is null) return false;

        if (member is Type resolvedType)
            return IsBearing(resolvedType, bearingNames, memo);

        try
        {
            if (member.DeclaringType is { } declaring && IsBearing(declaring, bearingNames, memo))
                return true;

            if (member is MethodInfo method && IsBearing(method.ReturnType, bearingNames, memo))
                return true;
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            return false;
        }

        return false;
    }

    private static MemberInfo? ResolveMemberOrNull(Module module, int token, Type[]? typeArguments, Type[]? methodArguments)
    {
        try
        {
            return module.ResolveMember(token, typeArguments, methodArguments);
        }
        catch (Exception ex) when (ex is ArgumentException or MissingMemberException or BadImageFormatException
            or TypeLoadException or FileNotFoundException or FileLoadException or NotSupportedException)
        {
            // A token that names a member in an assembly this test run does
            // not load cannot be classified. That is not a signal either way.
            return null;
        }
    }

    // Resolved once per (module, token). Static because the same framework
    // modules are walked by every enrolled project's gate in a shared run, and
    // concurrent because NUnit may run fixtures in parallel.
    private static readonly ConcurrentDictionary<(Module Module, int Token), MemberInfo?> ResolvedTokens = new();

    private static readonly Lazy<Dictionary<short, OpCode>> OpCodeTable = new(BuildOpCodeTable);

    private static Dictionary<short, OpCode> BuildOpCodeTable()
    {
        var fields = typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static);
        var table = new Dictionary<short, OpCode>(fields.Length);

        foreach (var field in fields)
            if (field.GetValue(null) is OpCode opCode)
                table[opCode.Value] = opCode;

        return table;
    }

    /// <summary>
    /// True when <paramref name="type"/>, or any of its base types or
    /// implemented interfaces, is named in <paramref name="bearingNames"/>, or
    /// is a user-defined <c>*ClusterFixture</c> helper.
    /// </summary>
    private static bool IsBearing(Type type, IReadOnlySet<string> bearingNames, Dictionary<Type, bool> memo)
    {
        if (memo.TryGetValue(type, out var known)) return known;

        var result = ComputeBearing(type, bearingNames);
        memo[type] = result;
        return result;
    }

    private static bool ComputeBearing(Type type, IReadOnlySet<string> bearingNames)
    {
        // A by-ref local (`ref var x = ...`), a pointer, or an array of hosts
        // all carry the element type's meaning.
        var current = type;
        while (current.HasElementType)
        {
            var element = current.GetElementType();
            if (element is null) break;
            current = element;
        }

        if (MatchesName(current, bearingNames)) return true;

        try
        {
            for (var b = current.BaseType; b is not null && b != typeof(object); b = b.BaseType)
                if (MatchesName(b, bearingNames)) return true;

            var interfaces = current.GetInterfaces();
            for (var i = 0; i < interfaces.Length; i++)
                if (MatchesName(interfaces[i], bearingNames)) return true;
        }
        catch (Exception ex) when (IsMetadataFault(ex))
        {
            // A type whose base or interface list lives in an assembly that is
            // not on disk cannot be classified; treat it as not bearing rather
            // than failing the gate on an environment condition.
            return false;
        }

        return false;
    }

    private static bool MatchesName(Type type, IReadOnlySet<string> bearingNames)
    {
        if (type.FullName is { } name && bearingNames.Contains(name))
            return true;

        // Any user-defined helper whose simple name ends in "ClusterFixture" -
        // covers the EventStream / FaultInjection / FourShard / SmallLeaf /
        // MultiPageFourShard / MutationObserver / PublishEventsOverride /
        // TwoSite / PublicApiContract / PublicReplicationApi cluster fixtures
        // and any future siblings.
        return type.Name.EndsWith("ClusterFixture", StringComparison.Ordinal);
    }

    private static bool IsMetadataFault(Exception ex) =>
        ex is TypeLoadException or FileNotFoundException or FileLoadException
            or BadImageFormatException or NotSupportedException;

    /// <summary>
    /// Anti-vacuity control for the three passes added in #3142. Runs the real
    /// detector against <see cref="MethodBodyProbe"/> with a bearing set
    /// containing only the probe's own sentinel type, so each pass is proven
    /// end to end without depending on what the consuming assembly happens to
    /// contain.
    /// </summary>
    private static void AssertMethodBodyPassesAreLive()
    {
        var probeNames = new HashSet<string>(StringComparer.Ordinal)
        {
            typeof(MethodBodyProbe.ProbeSentinel).FullName!,
        };

        var memo = new Dictionary<Type, bool>();

        Assert.That(DeclaresBearingLocal(typeof(MethodBodyProbe), probeNames, memo), Is.True,
            "HYGIENE GATE VACUOUS: the method-body local pass did not find the sentinel local that "
            + $"'{nameof(MethodBodyProbe)}.{nameof(MethodBodyProbe.HoldsSynchronousLocal)}' exists solely to "
            + "declare. MethodBase.GetMethodBody / LocalVariables is therefore not returning usable "
            + "metadata in this environment, and every 'no method-body signal' verdict this gate reached "
            + "is meaningless. Fix the pass; do not delete this assertion.");

        Assert.That(NestedTypeBears(typeof(MethodBodyProbe), probeNames, memo, depth: 0), Is.True,
            "HYGIENE GATE VACUOUS: the compiler-generated capture pass did not find the sentinel that "
            + $"'{nameof(MethodBodyProbe)}.{nameof(MethodBodyProbe.HoldsHoistedLocalAsync)}' holds live across "
            + "an await, so the async state machine that local is lowered into was not reached. Hosts held "
            + "across an await - the single commonest shape in this repository - would go undetected. "
            + "Fix the walk; do not delete this assertion.");

        // The IL pass is asserted against ONE method rather than the whole
        // probe type, deliberately. Scanning the type would also meet
        // HoldsSynchronousLocal's `newobj ProbeSentinel`, whose declaring type
        // is bearing, so the assertion would still pass with return-type
        // resolution completely broken - and return-type resolution is the
        // half that catches a host constructed and handed straight on, which
        // is the only reason this pass exists.
        var callSiteProbe = typeof(MethodBodyProbe).GetMethod(
            nameof(MethodBodyProbe.CallsSentinelWithoutLocal),
            BindingFlags.Public | BindingFlags.Static);

        Assert.That(callSiteProbe, Is.Not.Null,
            $"HYGIENE GATE VACUOUS: the IL-pass probe method '{nameof(MethodBodyProbe.CallsSentinelWithoutLocal)}' "
            + "was not found, so the control below proves nothing.");

        Assert.That(AnyBearingCallSite([callSiteProbe!], probeNames, memo), Is.True,
            $"HYGIENE GATE VACUOUS: the IL call-site pass did not resolve the sentinel factory that "
            + $"'{nameof(MethodBodyProbe)}.{nameof(MethodBodyProbe.CallsSentinelWithoutLocal)}' calls without "
            + "storing the result. Either the IL walk desynchronised or Module.ResolveMember is failing, and "
            + "every fixture that builds a host in a single expression - with no field, property, local or "
            + "capture to show for it - would go undetected. Fix the pass; do not delete this assertion.");
    }

    /// <summary>
    /// Sentinel workload for the anti-vacuity control. Every construct here is
    /// load-bearing: the sentinel is used across a loop back-edge so the
    /// compiler must give it a real local slot rather than folding it onto the
    /// evaluation stack, and it is live across the <c>await</c> so it must be
    /// hoisted into the state machine rather than staying a local.
    /// </summary>
    private static class MethodBodyProbe
    {
        internal sealed class ProbeSentinel
        {
            public int Weight => 1;
        }

        public static int HoldsSynchronousLocal()
        {
            var sentinel = new ProbeSentinel();
            var total = 0;
            for (var i = 0; i < 2; i++) total += sentinel.Weight;
            return total;
        }

        public static async Task<int> HoldsHoistedLocalAsync()
        {
            var sentinel = new ProbeSentinel();
            await Task.Yield();
            return sentinel.Weight;
        }

        // Constructed and consumed in one expression, so the sentinel occupies
        // no field, property, local or capture anywhere. Only the call site
        // survives, which is what the IL pass reads.
        public static int CallsSentinelWithoutLocal() => Consume(CreateSentinel());

        private static ProbeSentinel CreateSentinel() => new();

        private static int Consume(ProbeSentinel sentinel) => sentinel.Weight;
    }

    private static bool HasTestFixtureAttribute(Type type) =>
        type.GetCustomAttributes<TestFixtureAttribute>(inherit: true).Any();

    private static HashSet<string> GetCategoryNames(Type type) =>
        type.GetCustomAttributes<CategoryAttribute>(inherit: true)
            .Select(a => a.Name)
            .ToHashSet(StringComparer.Ordinal);

    private static IEnumerable<Type> SafeGetTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException ex) { return ex.Types.Where(t => t is not null)!; }
    }
}
