using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for a saga terminal delivered again after newer writes to
/// its keys. A terminal is delivered more than once by design - a saga re-runs
/// its broadcast when it resumes after its decision, and the split-forward
/// channel delivers a second subset - and each repeat carries the committed
/// values as a backstop for keys the leaf never prepared. Once the leaf had
/// drained the saga's bucket, every key of a repeat counted as missing and was
/// backstopped with a stamp above every row, so a repeat arriving after newer
/// writes resurrected the saga's older values over them.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly byte[] SagaRound = [1];
    private static readonly byte[] NewerRound = [2];

    private static Dictionary<string, byte[]> Committed(params string[] keys)
    {
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var key in keys)
            committed[key] = SagaRound;
        return committed;
    }

    [Test]
    public async Task ApplyTxTerminalAsync_repeat_after_a_newer_write_does_not_resurrect_a_drained_saga()
    {
        var grain = CreateGrain();
        var saga = Guid.NewGuid();
        await PreparedSetAsync(grain, saga, "k", SagaRound);
        await grain.ApplyTxTerminalAsync(saga, committed: true, Committed("k"));
        Assert.That(await grain.GetAsync("k"), Is.EqualTo(SagaRound));

        await grain.SetAsync("k", NewerRound);
        await grain.ApplyTxTerminalAsync(saga, committed: true, Committed("k"));

        Assert.That(await grain.GetAsync("k"), Is.EqualTo(NewerRound),
            "A repeated terminal must not overwrite a write made after the saga landed here.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_payload_after_an_empty_landing_does_not_overwrite_a_later_write()
    {
        // The first delivery carried no payload but still landed the saga here;
        // a write after it is newer than the saga, whose decision preceded it.
        var grain = CreateGrain();
        var saga = Guid.NewGuid();
        await grain.ApplyTxTerminalAsync(saga, committed: true, committedValues: null);

        await grain.SetAsync("k", NewerRound);
        await grain.ApplyTxTerminalAsync(saga, committed: true, Committed("k"));

        Assert.That(await grain.GetAsync("k"), Is.EqualTo(NewerRound));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_repeat_still_backstops_a_key_the_saga_never_reached_here()
    {
        // The control: a second subset naming a key this leaf has not seen the
        // saga write - absent, or older than the landing - is still backstopped.
        var grain = CreateGrain();
        var saga = Guid.NewGuid();
        await grain.SetAsync("other", [9]);
        await PreparedSetAsync(grain, saga, "k", SagaRound);
        await grain.ApplyTxTerminalAsync(saga, committed: true, Committed("k"));

        await grain.ApplyTxTerminalAsync(saga, committed: true, Committed("absent", "other"));

        Assert.That(await grain.GetAsync("absent"), Is.EqualTo(SagaRound));
        Assert.That(await grain.GetAsync("other"), Is.EqualTo(SagaRound),
            "A row older than the saga's landing predates the saga, so the backstop wins.");
    }
}
