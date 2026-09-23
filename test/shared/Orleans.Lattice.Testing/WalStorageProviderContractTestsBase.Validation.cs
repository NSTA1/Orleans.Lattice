using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Argument-validation half of <see cref="WalStorageProviderContractTestsBase"/>.
/// Every member of the seam documents a non-null tree id and a cancellation
/// token observed before any I/O.
/// </summary>
public abstract partial class WalStorageProviderContractTestsBase
{
    /// <summary>Every provider operation, by name, for the validation test cases.</summary>
    protected static readonly string[] Operations =
    [
        "AppendBatch",
        "AppendEncodedBatch",
        "Read",
        "ReadEncoded",
        "GetHighestOffset",
        "GetLowestOffset",
        "Trim",
        "EvaluateCompaction",
        "Reconcile",
        "GetRetainedByteSize",
        "GetPhysicalByteSize",
    ];

    [TestCaseSource(nameof(Operations))]
    public async Task Every_operation_rejects_a_null_tree_id(string operation)
    {
        await using var probe = await CreateProbeAsync();

        Assert.That(
            async () => await InvokeAsync(probe, operation, null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    /// <summary>
    /// The shard is seeded first because <c>ReadAsync</c> documents its token as
    /// observed between yielded entries, not before the scan, so a read of an
    /// empty shard may legitimately return without checking it. Seeding also
    /// makes "a cancelled mutation changes nothing" checkable for trims.
    /// </summary>
    [TestCaseSource(nameof(Operations))]
    public async Task Every_operation_observes_a_pre_cancelled_token(string operation)
    {
        await using var probe = await CreateProbeAsync();
        var seeded = Entries(0, 3);
        await probe.AppendAsync(TreeId, Shard, seeded, CancellationToken.None);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        Assert.That(
            async () => await InvokeAsync(probe, operation, TreeId, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(
            Describe(await ReadAllAsync(probe)),
            Is.EqualTo(Describe(seeded)),
            "A cancelled operation must leave the shard unchanged.");
    }

    [Test]
    public async Task Encoded_append_rejects_mismatched_offsets_and_writes_nothing()
    {
        await using var probe = await CreateProbeAsync();

        Assert.That(
            async () => await probe.AppendEncodedWithMismatchedOffsetsAsync(TreeId, Shard, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());

        Assert.Multiple(async () =>
        {
            Assert.That(await ReadAllAsync(probe), Is.Empty);
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));
        });
    }

    private static Task InvokeAsync(
        IWalStorageProviderContractProbe probe,
        string operation,
        string treeId,
        CancellationToken cancellationToken) => operation switch
        {
            "AppendBatch" => probe.AppendAsync(treeId, Shard, Entries(10, 2), cancellationToken),
            "AppendEncodedBatch" => probe.AppendEncodedAsync(treeId, Shard, Entries(10, 2), cancellationToken),
            "Read" => probe.ReadAsync(treeId, Shard, -1L, 16, cancellationToken),
            "ReadEncoded" => probe.ReadEncodedAsync(treeId, Shard, -1L, 16, cancellationToken),
            "GetHighestOffset" => probe.GetHighestOffsetAsync(treeId, Shard, cancellationToken),
            "GetLowestOffset" => probe.GetLowestOffsetAsync(treeId, Shard, cancellationToken),
            "Trim" => probe.TrimAsync(treeId, Shard, 0L, cancellationToken),
            "EvaluateCompaction" => probe.EvaluateCompactionAsync(treeId, Shard, cancellationToken),
            "Reconcile" => probe.ReconcileAsync(treeId, Shard, cancellationToken),
            "GetRetainedByteSize" => probe.GetRetainedByteSizeAsync(treeId, Shard, cancellationToken),
            "GetPhysicalByteSize" => probe.GetPhysicalByteSizeAsync(treeId, Shard, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown provider operation."),
        };
}
