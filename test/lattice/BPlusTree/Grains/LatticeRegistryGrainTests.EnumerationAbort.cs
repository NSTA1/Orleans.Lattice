using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the registry catalog scan surviving an
/// <see cref="EnumerationAbortedException"/> raised mid-enumeration.
/// <para>
/// The registry tree is backed by <c>LatticeGrain</c>, which is
/// <c>[StatelessWorker]</c>. Orleans keeps async-enumerable state in a dictionary
/// on the activation that served <c>StartEnumeration</c>, while every subsequent
/// <c>MoveNext</c> is an independent grain message routed with no request
/// affinity, so a <c>MoveNext</c> can land on a sibling worker that holds no
/// state for the enumerator and abort the scan. That is a steady-state,
/// load-proportional rate rather than a rare failover event, and a single-page
/// scan is fully exposed to it.
/// </para>
/// <para>
/// These fixtures assert the observable contract that matters to callers - that
/// <c>GetAllTreeIdsAsync</c> returns the <em>complete</em> catalog in order
/// across an induced abort. They deliberately do not assert that a wrapper was
/// called: a fixture that only proved the wrapper retries would stay green if
/// the registry stopped using it, which is precisely the defect being fixed.
/// </para>
/// </summary>
public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task GetAllTreeIdsAsync_returns_the_complete_catalog_across_an_enumerator_abort()
    {
        var (grain, tree) = CreateGrain();
        var starts = new List<string?>();
        var callIndex = 0;
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                return callIndex++ == 0
                    ? KeysThenAbort("alpha", "beta")
                    : ToAsyncEnumerable("gamma", "delta");
            });

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.EqualTo(new[] { "alpha", "beta", "gamma", "delta" }),
            "the abort must be absorbed and the catalog completed - no duplicates, no gaps");
        Assert.That(starts[0], Is.Null, "the first segment opens on the unbounded range");
        Assert.That(starts[1], Is.EqualTo("beta\u0000"),
            "the resumed segment opens at the successor of the last yielded key");
    }

    [Test]
    public async Task GetAllTreeIdsAsync_still_excludes_system_trees_on_a_resumed_segment()
    {
        var (grain, tree) = CreateGrain();
        var callIndex = 0;
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => callIndex++ == 0
                ? KeysThenAbort("alpha")
                : ToAsyncEnumerable(LatticeConstants.SystemTreePrefix + "internal", "beta"));

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.EqualTo(new[] { "alpha", "beta" }),
            "the reserved system-tree namespace stays excluded after a reopen");
    }

    [Test]
    public async Task GetAllTreeIdsAsync_with_a_prefix_resumes_inside_the_original_bounds()
    {
        var (grain, tree) = CreateGrain();
        var starts = new List<string?>();
        var ends = new List<string?>();
        var callIndex = 0;
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                ends.Add(ci.ArgAt<string?>(1));
                return callIndex++ == 0
                    ? KeysThenAbort("t/acme/one")
                    : ToAsyncEnumerable("t/acme/two");
            });

        var result = await grain.GetAllTreeIdsAsync("t/acme/");

        Assert.That(result, Is.EqualTo(new[] { "t/acme/one", "t/acme/two" }));
        Assert.That(starts[1], Is.EqualTo("t/acme/one\u0000"),
            "the resume tightens the lower bound only");
        Assert.That(ends[1], Is.EqualTo(ends[0]),
            "the caller-supplied upper bound is preserved across the reopen, so a resume can never widen the scan");
    }

    [Test]
    public void GetAllTreeIdsAsync_surfaces_the_abort_once_the_reconnect_budget_is_exhausted()
    {
        var (grain, tree) = CreateGrain();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => KeysThenAbort());

        // Recovery is bounded, not unconditional: a permanently aborting scan
        // must still terminate and report rather than spin forever.
        Assert.ThrowsAsync<EnumerationAbortedException>(async () => await grain.GetAllTreeIdsAsync());
    }

    private static async IAsyncEnumerable<string> KeysThenAbort(params string[] items)
    {
        foreach (var item in items)
        {
            yield return item;
            await Task.Yield();
        }
        throw new EnumerationAbortedException();
    }
}
