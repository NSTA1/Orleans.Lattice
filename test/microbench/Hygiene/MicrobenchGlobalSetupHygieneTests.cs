using System.Collections;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Hygiene;

/// <summary>
/// The microbench activation-wiring gate: every BenchmarkDotNet class in the
/// harness must be able to complete its <c>[GlobalSetup]</c>.
/// <para>
/// A <c>[GlobalSetup]</c> that throws does not fail a bench run in any way a
/// reader notices. BenchmarkDotNet reports <c>ExitCode != 0 and no results
/// reported</c> for the affected class and the surrounding harness carries on,
/// so <c>benchmark/performance-report.ps1</c> aggregates zero samples, emits
/// <c>[aggregate-l1] no p50 samples</c> warnings, exits 0, and regenerates the
/// <c>perf-table:layer1</c> block of
/// <c>docs/lattice/performance-single-silo.md</c> empty. That is precisely the
/// shape of a false green: the published document loses half its content and
/// every check stays green. Issue #3126 is the instance - the harness had been
/// silently publishing an empty Layer 1 table because
/// <see cref="Orleans.Lattice.BPlusTree.IShardHealingOrchestratorGrain"/> was
/// reachable from <c>LatticeGrain</c>'s activation path but had no route in
/// <c>FakeGrainFactory</c>.
/// </para>
/// <para>
/// The regression vector is not the benchmark harness. It is
/// <c>src/lattice</c>: <c>FakeGrainFactory</c> throws on any unrouted grain
/// interface by design, so adding a grain to the activation path of
/// <c>LatticeGrain</c> - a change that need not touch <c>benchmark/</c> at all -
/// breaks every benchmark class that seeds through the public
/// <c>ILattice</c> surface. The predecessor NSubstitute auto-mock returned
/// <c>Task.CompletedTask</c> for anything, which is why this never used to
/// matter and why nothing was watching for it.
/// </para>
/// <para>
/// This fixture therefore lives in a content-gate directory and namespace so
/// the unconditional <c>content-gates</c> job selects it on every pull request,
/// whatever the diff touched. The dedicated benchmark lane in <c>ci.yml</c>
/// only fires on a <c>benchmark/**</c> change, which is the one diff shape that
/// cannot produce this defect.
/// </para>
/// </summary>
/// <remarks>
/// Each class is driven exactly the way BenchmarkDotNet drives it: construct
/// through the public parameterless constructor, assign every
/// <c>[Params]</c>-decorated member its first declared value, then invoke every
/// <c>[GlobalSetup]</c> method. The harness's own environment knobs are turned
/// down for the duration so the gate costs seconds rather than the minutes a
/// full 10,000-key seed per class would.
/// </remarks>
[TestFixture]
public sealed class MicrobenchGlobalSetupHygieneTests
{
    /// <summary>
    /// The class the Layer 1 rows of <c>performance-single-silo.md</c> are
    /// measured from. Named explicitly so a rename cannot leave this gate
    /// scanning a population that no longer includes the class it was written
    /// for.
    /// </summary>
    private const string Layer1BenchmarkClass = nameof(LatticeMicroBenchmarks);

    /// <summary>
    /// A deliberately loose floor on the discovered population. It is a vacuity
    /// control, not a census: it fires when the scan has stopped finding
    /// benchmark classes at all (a moved assembly, a changed attribute) rather
    /// than when one is added or removed.
    /// </summary>
    private const int MinimumExpectedBenchmarkClasses = 20;

    /// <summary>
    /// Harness knobs turned down for the gate. Every value still exercises the
    /// same wiring - the grain routes a seed reaches are a function of the code
    /// path, not of the keyspace size - while keeping the whole gate inside a
    /// few seconds.
    /// </summary>
    private static readonly (string Name, string Value)[] ScaledDownEnvironment =
    [
        ("BENCH_MICROBENCH_KEY_COUNT", "64"),
        ("BENCH_MICROBENCH_BULK_BATCH", "16"),
        ("BENCH_MICROBENCH_DEEPER_KEY_COUNT", "64"),
        ("BENCH_MICROBENCH_DEEPER_BULK_BATCH", "16"),
        ("BENCH_MICROBENCH_PROFILE", "off"),
    ];

    private readonly Dictionary<string, string?> _savedEnvironment = [];

    [OneTimeSetUp]
    public void ScaleTheHarnessDown()
    {
        foreach (var (name, value) in ScaledDownEnvironment)
        {
            _savedEnvironment[name] = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
        }
    }

    [OneTimeTearDown]
    public void RestoreTheEnvironment()
    {
        foreach (var (name, saved) in _savedEnvironment)
        {
            Environment.SetEnvironmentVariable(name, saved);
        }

        _savedEnvironment.Clear();
    }

    /// <summary>
    /// Fails when the scan itself has stopped working, so the per-class gate
    /// below cannot pass by finding nothing to check.
    /// </summary>
    [Test]
    public void The_scan_finds_the_harness_benchmark_classes()
    {
        var discovered = DiscoverBenchmarkClasses();

        Assert.That(
            discovered,
            Is.Not.Empty,
            "found no BenchmarkDotNet class declaring a [GlobalSetup] in "
            + $"{typeof(LatticeMicroBenchmarks).Assembly.GetName().Name}. Either the harness moved or this "
            + "scan is broken; either way the per-class gate would be asserting nothing.");

        Assert.That(
            discovered.Select(type => type.Name),
            Does.Contain(Layer1BenchmarkClass),
            $"{Layer1BenchmarkClass} is the class the published Layer 1 table is measured from, and it is "
            + "not in the scanned population. Update this gate to name whatever replaced it.");

        Assert.That(
            discovered,
            Has.Count.GreaterThanOrEqualTo(MinimumExpectedBenchmarkClasses),
            $"only {discovered.Count} benchmark class(es) were discovered, which is below the vacuity floor "
            + $"of {MinimumExpectedBenchmarkClasses}. The harness has either shrunk dramatically or the scan "
            + "has stopped matching.");
    }

    /// <summary>
    /// Drives one benchmark class's <c>[GlobalSetup]</c> to completion.
    /// </summary>
    [TestCaseSource(nameof(BenchmarkClassCases))]
    public void Global_setup_completes(Type benchmarkClass)
    {
        object instance;
        try
        {
            instance = Activator.CreateInstance(benchmarkClass)!;
        }
        catch (Exception ex)
        {
            Assert.Fail(
                $"{benchmarkClass.Name} could not be constructed, so BenchmarkDotNet cannot run it either."
                + Environment.NewLine + Unwrap(ex));
            return;
        }

        try
        {
            AssignFirstParamsValues(instance, benchmarkClass);

            foreach (var setup in GlobalSetupMethods(benchmarkClass))
            {
                try
                {
                    AwaitIfAsync(setup.Invoke(instance, []));
                }
                catch (Exception ex)
                {
                    Assert.Fail(
                        $"{benchmarkClass.Name}.{setup.Name} threw. BenchmarkDotNet reports this as "
                        + "'ExitCode != 0 and no results reported' and the harness carries on, so every row "
                        + "this class measures regenerates empty in docs/lattice/performance-single-silo.md "
                        + "without failing anything. If the cause is a FakeGrainFactory route, register it in "
                        + "the class's GlobalSetup (see issue #3126)."
                        + Environment.NewLine + Unwrap(ex));
                }
            }
        }
        finally
        {
            RunGlobalCleanupBestEffort(instance, benchmarkClass);
        }
    }

    /// <summary>
    /// One NUnit case per benchmark class, so a failure names the class that
    /// broke rather than aborting the whole scan at the first one.
    /// </summary>
    private static IEnumerable BenchmarkClassCases() =>
        DiscoverBenchmarkClasses().Select(type => new TestCaseData(type).SetName($"Global_setup_completes({type.Name})"));

    /// <summary>
    /// Every public, concrete, parameterless-constructible class in the
    /// microbench assembly that declares at least one <c>[GlobalSetup]</c>.
    /// That is exactly the population BenchmarkDotNet can be asked to run.
    /// </summary>
    private static IReadOnlyList<Type> DiscoverBenchmarkClasses() =>
        typeof(LatticeMicroBenchmarks).Assembly
            .GetTypes()
            .Where(type => type is { IsClass: true, IsAbstract: false, IsPublic: true })
            .Where(type => type.GetConstructor(Type.EmptyTypes) is not null)
            .Where(type => GlobalSetupMethods(type).Count > 0)
            .OrderBy(type => type.FullName, StringComparer.Ordinal)
            .ToArray();

    private static IReadOnlyList<MethodInfo> GlobalSetupMethods(Type type) =>
        type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(method => method.GetCustomAttribute<GlobalSetupAttribute>() is not null)
            .Where(method => method.GetParameters().Length == 0)
            .OrderBy(method => method.Name, StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// Mirrors BenchmarkDotNet's own parameter injection: it assigns each
    /// <c>[Params]</c> member one of the declared values before calling
    /// <c>[GlobalSetup]</c>. Leaving them at their CLR defaults would make this
    /// gate assert on a configuration BenchmarkDotNet never runs (a zero-sized
    /// keyspace, an unnamed enum member), which is a different - and weaker -
    /// claim than the one on the tin.
    /// </summary>
    private static void AssignFirstParamsValues(object instance, Type type)
    {
        const BindingFlags Members = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        foreach (var field in type.GetFields(Members))
        {
            var values = field.GetCustomAttribute<ParamsAttribute>()?.Values;
            if (values is { Length: > 0 })
            {
                field.SetValue(instance, values[0]);
            }
        }

        foreach (var property in type.GetProperties(Members))
        {
            var values = property.GetCustomAttribute<ParamsAttribute>()?.Values;
            if (values is { Length: > 0 } && property.SetMethod is not null)
            {
                property.SetValue(instance, values[0]);
            }
        }
    }

    /// <summary>
    /// Releases whatever the setup acquired. A cleanup failure is not this
    /// gate's subject - BenchmarkDotNet has already produced its measurements
    /// by then - so it is swallowed rather than allowed to mask the setup
    /// result the test actually reports on.
    /// </summary>
    private static void RunGlobalCleanupBestEffort(object instance, Type type)
    {
        var cleanups = type
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(method => method.GetCustomAttribute<GlobalCleanupAttribute>() is not null)
            .Where(method => method.GetParameters().Length == 0);

        foreach (var cleanup in cleanups)
        {
            try
            {
                AwaitIfAsync(cleanup.Invoke(instance, []));
            }
            catch
            {
                // Deliberately ignored; see the summary.
            }
        }
    }

    private static void AwaitIfAsync(object? result)
    {
        switch (result)
        {
            case Task task:
                task.GetAwaiter().GetResult();
                break;
            case ValueTask valueTask:
                valueTask.GetAwaiter().GetResult();
                break;
        }
    }

    /// <summary>
    /// Reflection wraps whatever the setup threw in a
    /// <see cref="TargetInvocationException"/>, whose own message names no
    /// cause. The failure message is the entire diagnostic value of this gate,
    /// so the real exception is what gets reported.
    /// </summary>
    private static Exception Unwrap(Exception ex) =>
        ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
}
