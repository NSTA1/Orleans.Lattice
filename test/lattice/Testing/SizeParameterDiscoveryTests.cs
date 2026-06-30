using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

/// <summary>
/// Unit coverage for the reusable reflection discovery in
/// <see cref="SizeParameterDiscovery"/> and the
/// <see cref="SizeParameterTarget"/> record, exercised against a small sample
/// interface so the logic is verified without standing up a cluster.
/// </summary>
[TestFixture]
public sealed class SizeParameterDiscoveryTests
{
    private interface ISample
    {
        Task PageAsync(string cursorId, int pageSize, CancellationToken cancellationToken);

        Task DeleteAsync(string cursorId, int maxToDelete);

        // Two size parameters on one method -> two targets.
        Task ResizeAsync(int count, int limit);

        // Not size-like (name not in the set) -> ignored.
        Task RebuildAsync(int shardIndex);

        // Not an int -> ignored.
        Task RenameAsync(string size);

        int Total { get; }
    }

    [Test]
    public void Discover_finds_every_size_like_int_parameter()
    {
        var targets = SizeParameterDiscovery.Discover([typeof(ISample)]);

        var found = targets
            .Select(t => $"{t.Method.Name}.{t.Parameter.Name}")
            .ToArray();

        Assert.That(found, Is.EquivalentTo(new[]
        {
            "PageAsync.pageSize",
            "DeleteAsync.maxToDelete",
            "ResizeAsync.count",
            "ResizeAsync.limit",
        }));
    }

    [Test]
    public void Discover_ignores_non_int_and_non_size_named_parameters()
    {
        var targets = SizeParameterDiscovery.Discover([typeof(ISample)]);

        Assert.Multiple(() =>
        {
            Assert.That(targets.Any(t => t.Method.Name == "RebuildAsync"), Is.False,
                "shardIndex is not a size-like name and must be ignored.");
            Assert.That(targets.Any(t => t.Method.Name == "RenameAsync"), Is.False,
                "a string parameter named 'size' is not an int and must be ignored.");
        });
    }

    [Test]
    public void Discover_skips_property_accessors()
    {
        var targets = SizeParameterDiscovery.Discover([typeof(ISample)]);

        Assert.That(targets.Any(t => t.Method.IsSpecialName), Is.False,
            "compiler-generated property accessors must be skipped.");
    }

    [Test]
    public void Discover_honours_a_custom_size_name_set()
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "shardIndex" };

        var targets = SizeParameterDiscovery.Discover([typeof(ISample)], names);

        Assert.That(
            targets.Select(t => $"{t.Method.Name}.{t.Parameter.Name}"),
            Is.EqualTo(new[] { "RebuildAsync.shardIndex" }));
    }

    [Test]
    public void Discover_is_deterministically_ordered()
    {
        var first = SizeParameterDiscovery.Discover([typeof(ISample)]);
        var second = SizeParameterDiscovery.Discover([typeof(ISample)]);

        Assert.That(
            first.Select(t => t.DisplayName),
            Is.EqualTo(second.Select(t => t.DisplayName)));
    }

    [Test]
    public void Discover_throws_for_null_api_types()
    {
        Assert.That(
            () => SizeParameterDiscovery.Discover(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void PathologicalBoundaryValues_cover_the_documented_set()
    {
        Assert.That(
            SizeParameterDiscovery.PathologicalBoundaryValues,
            Is.EqualTo(new[] { int.MaxValue, int.MinValue, 0, -1 }));
    }

    [Test]
    public void DefaultSizeParameterNames_match_case_insensitively()
    {
        Assert.That(
            SizeParameterDiscovery.DefaultSizeParameterNames.Contains("PAGESIZE"),
            Is.True);
    }

    [Test]
    public void SizeParameterTarget_DisplayName_is_type_method_parameter()
    {
        var method = typeof(ISample).GetMethod(nameof(ISample.DeleteAsync))!;
        var parameter = method.GetParameters().Single(p => p.Name == "maxToDelete");
        var target = new SizeParameterTarget(typeof(ISample), method, parameter);

        Assert.That(target.DisplayName, Is.EqualTo("ISample.DeleteAsync(maxToDelete)"));
        Assert.That(target.ToString(), Is.EqualTo("ISample.DeleteAsync(maxToDelete)"));
    }

    [Test]
    public void ContractArgument_UseDefault_is_a_stable_singleton()
    {
        Assert.That(ContractArgument.UseDefault, Is.SameAs(ContractArgument.UseDefault));
    }
}
