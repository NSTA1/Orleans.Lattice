using Orleans.Lattice.Benchmark.Microbench.Profiling;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Profiling;

/// <summary>
/// Unit tests for <see cref="FrameFilter.IsProductFrame"/>, the predicate that
/// separates product-code frames from measurement-substrate noise during
/// stack-frame attribution.
/// </summary>
[TestFixture]
public sealed class FrameFilterTests
{
    [TestCase("Orleans.Lattice.VersionVector.Merge")]
    [TestCase("Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.SetAsync")]
    [TestCase("System.Collections.Generic.Dictionary`2.Resize")]
    [TestCase("Orleans.Serialization.Buffers.Writer`1.Commit")]
    public void IsProductFrame_accepts_product_and_framework_data_frames(string frame)
    {
        Assert.That(FrameFilter.IsProductFrame(frame), Is.True);
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("?")]
    [TestCase("?!UnknownModule!UnknownMethod")]
    [TestCase("UNMANAGED_CODE_TIME")]
    public void IsProductFrame_rejects_null_empty_and_unresolved(string? frame)
    {
        Assert.That(FrameFilter.IsProductFrame(frame), Is.False);
    }

    [TestCase("Castle.DynamicProxy.AbstractInvocation.Proceed")]
    [TestCase("NSubstitute.Core.CallRouter.Route")]
    [TestCase("BenchmarkDotNet.Engines.Engine.RunIteration")]
    [TestCase("Orleans.Lattice.Benchmark.Microbench.Profiling.ProfileAggregator.RecordAllocation")]
    public void IsProductFrame_rejects_mock_and_engine_frames(string frame)
    {
        Assert.That(FrameFilter.IsProductFrame(frame), Is.False);
    }

    [TestCase("System.Runtime.CompilerServices.AsyncTaskMethodBuilder.Start")]
    [TestCase("System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start")]
    [TestCase("System.Runtime.CompilerServices.AsyncValueTaskMethodBuilder`1.AwaitUnsafeOnCompleted")]
    [TestCase("System.Threading.ExecutionContext.RunInternal")]
    public void IsProductFrame_rejects_async_builder_plumbing(string frame)
    {
        Assert.That(FrameFilter.IsProductFrame(frame), Is.False);
    }
}
