using System.Reflection;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Unit coverage for the cluster-fixture detector inside
/// <see cref="IntegrationCategoryHygieneTestsBase"/>, and for the
/// <see cref="FastInProcessHostFixtureAttribute"/> exemption it honours.
/// <para>
/// Issue #3142. The detector used to look only at declared field and property
/// types, so a fixture that built its host inside a method body was invisible
/// to it - and a census found that every untagged host-building fixture in the
/// repository was exactly that shape, which is to say the gate was silent on
/// precisely the case it exists to catch. Detection is now four passes, and
/// each of them is pinned here against a purpose-built sample whose lowering
/// is the thing under test.
/// </para>
/// <para>
/// The samples deliberately carry a LOCAL sentinel type rather than a real
/// host type. Feeding the detector a bearing set of the test's own choosing
/// exercises the identical code path while keeping these samples invisible to
/// the production gate that scans this very assembly - which would otherwise
/// report every sample below as an untagged cluster fixture.
/// </para>
/// </summary>
[TestFixture]
public sealed class IntegrationCategoryDetectionTests
{
    // --- Sentinels the samples are built around ---------------------------

    private sealed class Sentinel
    {
        public int Weight => 1;
    }

    private interface ISentinelMarker;

    private sealed class DerivedSentinel : ISentinelMarker;

    // --- Samples, one per lowering the detector must see ------------------

    // The detector reads a field's declared TYPE, never its value, so these
    // sample fields are deliberately never assigned. CS0649 is exactly the
    // right warning for production code and exactly the wrong one here.
#pragma warning disable CS0649

    private sealed class HoldsField
    {
        public Sentinel? Held;
    }

    private sealed class HoldsProperty
    {
        public Sentinel? Held { get; set; }
    }

    private sealed class HoldsSynchronousLocal
    {
        public static int Run()
        {
            var sentinel = new Sentinel();
            var total = 0;
            for (var i = 0; i < 2; i++) total += sentinel.Weight;
            return total;
        }
    }

    private sealed class HoldsLocalAcrossAwait
    {
        public static async Task<int> RunAsync()
        {
            var sentinel = new Sentinel();
            await Task.Yield();
            return sentinel.Weight;
        }
    }

    private sealed class CapturesLocalInLambda
    {
        public static Func<int> Run()
        {
            var sentinel = new Sentinel();
            return () => sentinel.Weight;
        }
    }

    private sealed class ConstructsAndPassesInOneExpression
    {
        public static int Run() => Consume(Create());

        private static Sentinel Create() => new();

        private static int Consume(Sentinel sentinel) => sentinel.Weight;
    }

    private sealed class HoldsSubtypeOfBearingInterface
    {
        public DerivedSentinel? Held;
    }

    private class BearingBase
    {
        protected Sentinel? Held;
    }

    private sealed class InheritsSignalFromBase : BearingBase;

    private sealed class MentionsOnlyTypeof
    {
        public static string Run() => typeof(Sentinel).FullName ?? string.Empty;
    }

    private sealed class HoldsOnlyAGenericArgument
    {
        public List<Sentinel>? Held;
    }

    private sealed class TouchesNothing
    {
        public static int Run()
        {
            var total = 0;
            for (var i = 0; i < 3; i++) total += i;
            return total;
        }
    }

#pragma warning restore CS0649

    // --- The detector, reached through its public seam --------------------

    private static readonly string[] SentinelNames =
    [
        typeof(Sentinel).FullName!,
    ];

    private static readonly string[] MarkerNames =
    [
        typeof(ISentinelMarker).FullName!,
    ];

    private static string? Detect(Type sample, string[]? bearingNames = null) =>
        IntegrationCategoryHygieneTestsBase.DescribeClusterSignal(
            sample,
            new HashSet<string>(bearingNames ?? SentinelNames, StringComparer.Ordinal));

    // --- Detection: the four passes ---------------------------------------

    [Test]
    public void A_bearing_field_is_detected_as_a_declared_member() =>
        Assert.That(Detect(typeof(HoldsField)), Is.EqualTo("field/property type"));

    [Test]
    public void A_bearing_property_is_detected_as_a_declared_member() =>
        Assert.That(Detect(typeof(HoldsProperty)), Is.EqualTo("field/property type"));

    [Test]
    public void A_bearing_synchronous_local_is_detected_as_a_method_body_local() =>
        Assert.That(Detect(typeof(HoldsSynchronousLocal)), Is.EqualTo("method-body local"));

    /// <summary>
    /// The commonest real shape in this repository:
    /// <c>await using var app = builder.Build();</c>. The local is live across
    /// the await, so the compiler hoists it out of the method body into a
    /// field of the async state machine, where the method-body pass can no
    /// longer see it.
    /// </summary>
    [Test]
    public void A_bearing_local_held_across_an_await_is_detected_as_a_capture() =>
        Assert.That(Detect(typeof(HoldsLocalAcrossAwait)),
            Is.EqualTo("compiler-generated capture (async state machine or lambda closure)"));

    [Test]
    public void A_bearing_local_captured_by_a_lambda_is_detected_as_a_capture() =>
        Assert.That(Detect(typeof(CapturesLocalInLambda)),
            Is.EqualTo("compiler-generated capture (async state machine or lambda closure)"));

    /// <summary>
    /// The pass that forced IL reading. A value constructed and consumed in a
    /// single expression - <c>Consume(Create())</c>, the lowering of
    /// <c>LatticeGrpcChannelFactory.CreateCallInvoker(GrpcChannel.ForAddress(...))</c> -
    /// occupies no field, no property, no local and no capture, so the three
    /// cheaper passes all correctly decline and only the call site remains.
    /// If this test regresses to <see langword="null"/> the IL walk has broken
    /// and <c>StaticCredentialTransportGateTests</c> is invisible again.
    /// </summary>
    [Test]
    public void A_bearing_value_constructed_and_consumed_in_one_expression_is_detected_as_an_IL_call_site() =>
        Assert.That(Detect(typeof(ConstructsAndPassesInOneExpression)), Is.EqualTo("IL call site"));

    // --- Detection: transitive matching -----------------------------------

    /// <summary>
    /// Load-bearing rather than defensive: the local in
    /// <c>await using var app = builder.Build()</c> is a
    /// <c>WebApplication</c>, whose own name is not in the bearing set and
    /// never will be. It matches because it implements <c>IHost</c>.
    /// </summary>
    [Test]
    public void A_subtype_is_detected_through_an_implemented_bearing_interface() =>
        Assert.That(Detect(typeof(HoldsSubtypeOfBearingInterface), MarkerNames),
            Is.EqualTo("field/property type"));

    [Test]
    public void A_signal_declared_on_a_base_class_is_detected() =>
        Assert.That(Detect(typeof(InheritsSignalFromBase)), Is.EqualTo("field/property type"));

    // --- Precision: the things that must NOT be detected -------------------

    [Test]
    public void A_type_that_touches_no_bearing_type_is_not_detected() =>
        Assert.That(Detect(typeof(TouchesNothing)), Is.Null);

    /// <summary>
    /// Regression for a measured false positive.
    /// <c>DocsSnippetCompilationTestsBase</c> writes <c>typeof(IHost)</c> to
    /// locate the ASP.NET shared framework for its metadata reference set,
    /// which emits <c>ldtoken IHost</c>. An earlier revision of the IL pass
    /// read <c>InlineTok</c> operands and duly reported a Roslyn compilation
    /// fixture as a cluster fixture. Naming a type is not building one.
    /// </summary>
    [Test]
    public void Naming_a_bearing_type_with_typeof_is_not_detected() =>
        Assert.That(Detect(typeof(MentionsOnlyTypeof)), Is.Null);

    /// <summary>
    /// Documents a deliberate limit rather than an oversight. Unwrapping
    /// generic arguments would catch a <c>Task&lt;IHost&gt;</c>, but it would
    /// equally catch a <c>Mock&lt;IHost&gt;</c> in a pure unit test, which
    /// builds no host at all. The recall given up is nil, because a real host
    /// held across an await is already reached as the hoisted local itself.
    /// </summary>
    [Test]
    public void A_bearing_type_appearing_only_as_a_generic_argument_is_not_detected() =>
        Assert.That(Detect(typeof(HoldsOnlyAGenericArgument)), Is.Null);

    // --- The exemption attribute ------------------------------------------

    [Test]
    public void The_exemption_attribute_round_trips_its_justification()
    {
        const string justification = "In-memory TestServer only; measured 36 ms across 5 tests.";
        var attribute = new FastInProcessHostFixtureAttribute(justification);

        Assert.That(attribute.Justification, Is.EqualTo(justification));
    }

    [Test]
    public void The_exemption_attribute_refuses_an_absent_justification()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => _ = new FastInProcessHostFixtureAttribute(null!));
            Assert.Throws<ArgumentException>(() => _ = new FastInProcessHostFixtureAttribute(string.Empty));
            Assert.Throws<ArgumentException>(() => _ = new FastInProcessHostFixtureAttribute("   "));
        });
    }

    [Test]
    public void The_exemption_attribute_is_declarable_once_on_a_class_only()
    {
        var usage = typeof(FastInProcessHostFixtureAttribute)
            .GetCustomAttribute<AttributeUsageAttribute>();

        Assert.That(usage, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(usage!.ValidOn, Is.EqualTo(AttributeTargets.Class));
            Assert.That(usage.AllowMultiple, Is.False);
            Assert.That(usage.Inherited, Is.False,
                "An inherited exemption would silently excuse every subclass of an exempted fixture, "
                + "including ones that grow a real cluster later.");
        });
    }

    // --- The exemption's substance check ----------------------------------

    private static bool IsSubstantive(string justification) =>
        FastInProcessHostFixtureAttribute.IsSubstantiveJustification(justification);

    [Test]
    public void A_justification_carrying_a_measurement_is_substantive() =>
        Assert.That(IsSubstantive("In-memory TestServer only; measured 36 ms across 5 tests."), Is.True);

    [Test]
    public void A_justification_with_no_measurement_in_it_is_rejected() =>
        Assert.That(IsSubstantive("This one is fine, it is really quick, honestly it is."), Is.False,
            "The only defensible reason to keep a host-building fixture in the fast loop is a "
            + "measurement, and a measurement has a number in it.");

    [Test]
    public void A_justification_too_short_to_argue_anything_is_rejected() =>
        Assert.That(IsSubstantive("36 ms"), Is.False);

    [Test]
    public void The_minimum_justification_length_is_the_one_the_gate_reports() =>
        Assert.That(FastInProcessHostFixtureAttribute.MinimumJustificationLength, Is.EqualTo(40));
}
