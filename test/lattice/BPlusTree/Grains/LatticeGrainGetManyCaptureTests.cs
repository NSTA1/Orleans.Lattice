using System.Reflection;
using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class LatticeGrainGetManyCaptureTests
{
    [Test]
    public void GetManyAsyncCore_retry_delegates_do_not_allocate_an_enclosing_bucket_capture()
    {
        AllocationContract.RequireOptimizedBuild(typeof(LatticeGrain).Assembly);

        // Inspect the compiled production lambdas, not a reproduction of their
        // bodies. Capturing the attempt's fastBucket through an enclosing
        // display class adds a separate allocation to every healthy read.
        var captures = typeof(LatticeGrain)
            .GetNestedTypes(BindingFlags.NonPublic)
            .Where(type => type.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Where(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Any(method => method.Name.StartsWith("<GetManyAsyncCore>b__", StringComparison.Ordinal)))
            .ToArray();

        Assert.That(captures, Is.Not.Empty,
            "GetManyAsyncCore's retry lambdas were renamed or removed; update this allocation guard.");
        var enclosingCaptures = captures
            .SelectMany(type => type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            .Where(field => field.FieldType.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(field => $"{field.DeclaringType!.Name}.{field.Name}")
            .ToArray();

        Assert.That(enclosingCaptures, Is.Empty,
            "Keep each retry's shard and key list in the same lexical scope instead of allocating a parent capture.");
    }
}
