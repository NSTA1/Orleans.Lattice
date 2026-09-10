using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

/// <summary>
/// Unit coverage for <see cref="AllocationContract"/>, the precondition an
/// allocation fixture asserts before it trusts a steady-state figure from an
/// asynchronous path.
/// <para>
/// The behaviour that matters is the <i>detection</i>, because the guard exists
/// to stop a compiler artifact being misread as a product regression. These
/// tests therefore pin the predicate against assemblies whose optimization
/// state is known independently of how this test project happens to be built,
/// and check the guard's pass-through behaviour, rather than asserting a
/// configuration-specific outcome that would only hold in one of the two builds
/// this repository routinely uses.
/// </para>
/// </summary>
[TestFixture]
public sealed class AllocationContractTests
{
    [Test]
    public void IsUnoptimized_is_false_for_an_assembly_compiled_with_optimizations()
    {
        // The runtime's own core library is always shipped optimized, so it is a
        // fixed reference point in both Debug and Release test runs.
        Assert.That(AllocationContract.IsUnoptimized(typeof(object).Assembly), Is.False);
    }

    [Test]
    public void IsUnoptimized_agrees_with_how_this_assembly_was_compiled()
    {
        // The one assembly whose optimization state this test can know at
        // compile time. Pinning the predicate against it is what proves it reads
        // the real DebuggableAttribute rather than always answering "optimized",
        // which is the failure mode that would silently disarm the guard.
        var assembly = typeof(AllocationContractTests).Assembly;

        var debuggable = assembly.GetCustomAttribute<DebuggableAttribute>();
        var expected = debuggable is not null && debuggable.IsJITOptimizerDisabled;

        Assert.That(AllocationContract.IsUnoptimized(assembly), Is.EqualTo(expected));
    }

    [Test]
    public void IsUnoptimized_rejects_a_null_assembly()
    {
        Assert.That(() => AllocationContract.IsUnoptimized(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void UnoptimizedAmong_returns_nothing_when_every_assembly_is_optimized()
    {
        Assert.That(
            AllocationContract.UnoptimizedAmong(typeof(object).Assembly, typeof(Enumerable).Assembly),
            Is.Empty);
    }

    [Test]
    public void UnoptimizedAmong_returns_every_unoptimized_assembly_in_the_order_supplied()
    {
        var self = typeof(AllocationContractTests).Assembly;

        var unoptimized = AllocationContract.UnoptimizedAmong(typeof(object).Assembly, self);

        // Debug and Release disagree on whether this assembly qualifies, so the
        // assertion is expressed against the predicate rather than against a
        // fixed expectation - the ordering and filtering are what is under test.
        Assert.That(
            unoptimized,
            Is.EqualTo(new[] { typeof(object).Assembly, self }.Where(AllocationContract.IsUnoptimized)));
    }

    [Test]
    public void UnoptimizedAmong_is_empty_for_no_assemblies()
    {
        Assert.That(AllocationContract.UnoptimizedAmong(), Is.Empty);
    }

    [Test]
    public void UnoptimizedAmong_rejects_a_null_array()
    {
        Assert.That(() => AllocationContract.UnoptimizedAmong(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void UnoptimizedAmong_rejects_a_null_element()
    {
        Assert.That(
            () => AllocationContract.UnoptimizedAmong(typeof(object).Assembly, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void RequireOptimizedBuild_passes_when_every_assembly_is_optimized()
    {
        Assert.That(
            () => AllocationContract.RequireOptimizedBuild(typeof(object).Assembly),
            Throws.Nothing);
    }

    [Test]
    public void RequireOptimizedBuild_passes_for_no_assemblies()
    {
        Assert.That(() => AllocationContract.RequireOptimizedBuild(), Throws.Nothing);
    }

    [Test]
    public void RequireOptimizedBuild_rejects_a_null_array()
    {
        Assert.That(
            () => AllocationContract.RequireOptimizedBuild(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void IsUnoptimized_is_true_for_an_assembly_marked_optimizations_disabled()
    {
        Assert.That(AllocationContract.IsUnoptimized(UnoptimizedAssembly()), Is.True);
    }

    [Test]
    public void UnoptimizedAmong_selects_only_the_unoptimized_assembly()
    {
        var unoptimized = UnoptimizedAssembly();

        Assert.That(
            AllocationContract.UnoptimizedAmong(typeof(object).Assembly, unoptimized),
            Is.EqualTo(new[] { unoptimized }));
    }

    [Test]
    public void RequireOptimizedBuild_skips_and_explains_when_an_assembly_is_unoptimized()
    {
        // The branch the guard exists for. It is exercised against a synthesized
        // assembly rather than against this one, so the coverage holds in both
        // Debug and Release instead of evaporating in whichever build the
        // fixture happens to run under. The continuous-integration decision is
        // supplied explicitly for the same reason: asserting the ambient one
        // would cover the ignore branch locally and the fail branch in CI, so
        // neither would ever be checked in the environment that runs the other.
        var ignored = Assert.Throws<IgnoreException>(
            () => AllocationContract.RequireOptimizedBuild(
                failInsteadOfIgnore: false, UnoptimizedAssembly()));

        Assert.Multiple(() =>
        {
            // NUnit reports an IgnoreException as a visible Skipped. An
            // Inconclusive result would be counted as neither passed, failed,
            // nor skipped, so the gate would vanish from every summary counter
            // while the run still printed a pass.
            Assert.That(ignored!.Message, Does.Contain("optimized build"));
            Assert.That(ignored.Message, Does.Contain(UnoptimizedAssemblyName));
            Assert.That(ignored.Message, Does.Contain("-c Release"),
                "The message has to name the remedy, or it just relocates the confusion it exists to end.");
        });
    }

    [Test]
    public void RequireOptimizedBuild_fails_rather_than_skipping_in_continuous_integration()
    {
        // CI builds every project with '--configuration Release', so an
        // unoptimized assembly there is a pipeline defect. Ignoring it would
        // leave an allocation gate that quietly skips itself while still
        // reading as coverage - the failure mode this whole type exists to
        // prevent, reintroduced one level up.
        var failure = Assert.Throws<AssertionException>(
            () => AllocationContract.RequireOptimizedBuild(
                failInsteadOfIgnore: true, UnoptimizedAssembly()));

        Assert.Multiple(() =>
        {
            Assert.That(failure!.Message, Does.Contain(UnoptimizedAssemblyName));
            Assert.That(failure.Message, Does.Contain("pipeline is misconfigured"));
        });
    }

    [Test]
    public void RequireOptimizedBuild_passes_in_continuous_integration_when_every_assembly_is_optimized()
    {
        Assert.That(
            () => AllocationContract.RequireOptimizedBuild(
                failInsteadOfIgnore: true, typeof(object).Assembly),
            Throws.Nothing);
    }

    [Test]
    public void RequireOptimizedBuild_with_an_explicit_decision_rejects_a_null_array()
    {
        Assert.That(
            () => AllocationContract.RequireOptimizedBuild(failInsteadOfIgnore: true, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void RunningInContinuousIntegration_reflects_the_GITHUB_ACTIONS_variable()
    {
        var expected = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"));

        Assert.That(AllocationContract.RunningInContinuousIntegration(), Is.EqualTo(expected));
    }

    private const string UnoptimizedAssemblyName = "Orleans.Lattice.Tests.UnoptimizedProbe";

    /// <summary>
    /// A dynamic assembly carrying the same <see cref="DebuggableAttribute"/>
    /// the C# compiler emits for a Debug build, so the guard's detection can be
    /// tested without depending on how this test project was compiled.
    /// </summary>
    private static Assembly UnoptimizedAssembly() =>
        AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName(UnoptimizedAssemblyName),
            AssemblyBuilderAccess.Run,
            [
                new CustomAttributeBuilder(
                    typeof(DebuggableAttribute).GetConstructor([typeof(DebuggableAttribute.DebuggingModes)])!,
                    [
                        DebuggableAttribute.DebuggingModes.Default
                        | DebuggableAttribute.DebuggingModes.DisableOptimizations,
                    ]),
            ]);
}
