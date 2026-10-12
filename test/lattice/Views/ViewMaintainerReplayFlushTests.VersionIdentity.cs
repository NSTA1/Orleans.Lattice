using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    private sealed class TimestampProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "timestamp-v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            if (mutation.Kind == MutationKind.Set)
            {
                yield return ViewWrite.Upsert(mutation.Key, [(byte)mutation.Timestamp.WallClockTicks],
                    mutation.Timestamp, sourceKey: mutation.Key);
            }
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot(bool loseResponse)
    {
        var transaction = Guid.NewGuid();
        var log = new List<LatticeMutation>
        {
            Ordinary(1, 50),
            Prepared(transaction, Key, 1, 20, 2, 0),
            Prepared(transaction, "j", 2, 40, 2, 1),
            Commit(transaction, 41),
        };
        var view = new FakeViewTree { LoseNextAtomicResponse = loseResponse };
        var h = Create(view, new FakePersistentState<ViewCheckpointState>(),
            registration: new ViewRegistration(ViewName, SourceTreeId, new TimestampProjection()));
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        var responseLost = false;
        for (var pass = 0; pass < 3; pass++)
        {
            try
            {
                await h.Grain.DrainAsync();
            }
            catch (TimeoutException) when (loseResponse && !responseLost)
            {
                responseLost = true;
            }
        }

        Assert.That(responseLost, Is.EqualTo(loseResponse));
        Assert.That(view.Snapshots.Where(s => s.ContainsKey(Key)).Select(s => s[Key][0]),
            Is.All.EqualTo(50),
            "equal source bytes do not authorize publishing an older revision's timestamp-sensitive projection");
        Assert.That(view.Rows["j"], Is.EqualTo(new byte[] { 40 }));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Source_confirmation_waits_for_absent_or_older_rows_even_after_the_terminal_horizon_is_consumed(bool absent)
    {
        var transaction = Guid.NewGuid();
        var log = new List<LatticeMutation>
        {
            Prepared(transaction, Key, 2, 20, 2, 0),
            Prepared(transaction, "j", 4, 40, 2, 1),
            Commit(transaction, 41),
        };
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        if (absent)
        {
            h.SourceRows.Remove(Key);
            h.SourceVersions.Remove(Key);
        }
        else
        {
            h.SourceRows[Key] = [1];
            h.SourceVersions[Key] = Clock(10);
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        Assert.That(view.Snapshots, Is.Empty,
            "a consumed terminal horizon does not prove an absent or older source row supersedes the prepare");
        Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));

        h.SourceRows[Key] = [2];
        h.SourceVersions[Key] = Clock(20);
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows["j"], Is.EqualTo(new byte[] { 4 }));
            Assert.That(view.Snapshots, Has.Count.EqualTo(1));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }

    [Test]
    public async Task Source_deferred_complete_batch_does_not_suppress_an_eligible_ordinary_survivor()
    {
        var transaction = Guid.NewGuid();
        var log = new List<LatticeMutation>
        {
            Ordinary(1, 5),
            Prepared(transaction, Key, 2, 20, 2, 0),
            Prepared(transaction, "j", 4, 40, 2, 1),
            Commit(transaction, 41),
        };
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        h.SourceRows[Key] = [1];
        h.SourceVersions[Key] = Clock(5);

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 1 }),
                "Terminal completeness cannot suppress the ordinary survivor of a source-deferred batch.");
            Assert.That(view.Rows.ContainsKey("j"), Is.False);
            Assert.That(view.AtomicOperations, Is.Empty);
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(0));
        });

        h.SourceRows[Key] = [2];
        h.SourceVersions[Key] = Clock(20);
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows["j"], Is.EqualTo(new byte[] { 4 }));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Source_confirmation_defers_same_HLC_contradictory_tombstone_kind_as_a_whole_batch(bool preparedDelete)
    {
        var transaction = Guid.NewGuid();
        var prepared = Prepared(transaction, Key, 2, 20, 2, 0);
        if (preparedDelete)
        {
            prepared = prepared with { Kind = MutationKind.Delete, Value = null };
        }
        var log = new List<LatticeMutation>
        {
            prepared,
            Prepared(transaction, "j", 4, 40, 2, 1),
            Commit(transaction, 41),
        };
        var view = new FakeViewTree();
        view.Rows[Key] = [1];
        view.Rows["j"] = [1];
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        h.SourceVersions[Key] = Clock(20);
        if (preparedDelete)
        {
            h.SourceRows[Key] = [2];
        }
        else
        {
            h.SourceRows.Remove(Key);
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(view.Snapshots, Is.Empty,
                "Equal HLC with contradictory deletion kind is neither the prepared tuple nor strictly newer evidence.");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 1 }));
            Assert.That(view.Rows["j"], Is.EqualTo(new byte[] { 1 }));
            Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
        });

        if (preparedDelete)
        {
            h.SourceRows.Remove(Key);
        }
        else
        {
            h.SourceRows[Key] = [2];
        }
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(view.Rows.ContainsKey(Key), Is.EqualTo(!preparedDelete));
            Assert.That(view.Rows["j"], Is.EqualTo(new byte[] { 4 }));
            Assert.That(view.Snapshots, Has.Count.EqualTo(1));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }
}
