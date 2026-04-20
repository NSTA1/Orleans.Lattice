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

public partial class TreeSnapshotGrainTests
{
    [TestFixture]
    public class AbortAsyncTests
    {
        [Test]
        public void AbortAsync_throws_when_operationId_is_null_or_empty()
        {
            var (grain, _, _, _, _) = CreateGrain();
            Assert.That(
                async () => await grain.AbortAsync(null!),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await grain.AbortAsync(""),
                Throws.InstanceOf<ArgumentException>());
        }

        [Test]
        public async Task AbortAsync_is_idempotent_when_no_snapshot_is_in_progress()
        {
            var (grain, state, _, _, _) = CreateGrain();
            Assert.That(state.State.InProgress, Is.False);

            var writesBefore = state.WriteCount;
            await grain.AbortAsync("any-op");

            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
        }

        [Test]
        public async Task AbortAsync_is_noop_when_operationId_does_not_match()
        {
            var existingState = new FakePersistentState<TreeSnapshotState>();
            existingState.State.InProgress = true;
            existingState.State.DestinationTreeId = DestTreeId;
            existingState.State.OperationId = "op-original";
            existingState.State.Phase = SnapshotPhase.Copy;
            var (grain, state, reminderRegistry, _, _) = CreateGrain(existingState: existingState);

            await grain.AbortAsync("op-different");

            // State must remain unchanged — a stale coordinator must not tear
            // down a newer operation.
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.OperationId, Is.EqualTo("op-original"));
            Assert.That(state.State.DestinationTreeId, Is.EqualTo(DestTreeId));
            await reminderRegistry.DidNotReceive().UnregisterReminder(
                Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        }

        [Test]
        public async Task AbortAsync_clears_all_state_when_operationId_matches()
        {
            var existingState = new FakePersistentState<TreeSnapshotState>();
            existingState.State.InProgress = true;
            existingState.State.Complete = false;
            existingState.State.DestinationTreeId = DestTreeId;
            existingState.State.OperationId = "op-1";
            existingState.State.Phase = SnapshotPhase.Copy;
            existingState.State.NextShardIndex = 3;
            existingState.State.ShardRetries = 1;
            existingState.State.MaxLeafKeys = 64;
            existingState.State.MaxInternalChildren = 32;
            existingState.State.LogicalTreeId = "logical-name";
            var (grain, state, _, _, _) = CreateGrain(existingState: existingState);

            await grain.AbortAsync("op-1");

            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.DestinationTreeId, Is.Null);
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(state.State.ShardRetries, Is.EqualTo(0));
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Lock));
            Assert.That(state.State.MaxLeafKeys, Is.Null);
            Assert.That(state.State.MaxInternalChildren, Is.Null);
            Assert.That(state.State.LogicalTreeId, Is.EqualTo(""));
        }
    }
}