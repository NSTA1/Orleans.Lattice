using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeMergeGrainTests
{
    [TestFixture]
    public class IsCompleteAsyncTests
    {
        [Test]
        public async Task IsCompleteAsync_returns_true_when_no_merge_initiated()
        {
            var (grain, _, _, _, _) = CreateGrain();
            var result = await grain.IsCompleteAsync();
            Assert.That(result, Is.True);
        }

        [Test]
        public async Task IsCompleteAsync_returns_false_when_merge_in_progress()
        {
            var existingState = new FakePersistentState<TreeMergeState>();
            existingState.State.InProgress = true;
            existingState.State.SourceTreeId = SourceTreeId;
            existingState.State.SourceShardCount = ShardCount;
            var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
            var result = await grain.IsCompleteAsync();
            Assert.That(result, Is.False);
        }

        [Test]
        public async Task IsCompleteAsync_returns_true_after_merge_completes()
        {
            var existingState = new FakePersistentState<TreeMergeState>();
            existingState.State.InProgress = false;
            existingState.State.Complete = true;
            var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
            var result = await grain.IsCompleteAsync();
            Assert.That(result, Is.True);
        }
    }
}
