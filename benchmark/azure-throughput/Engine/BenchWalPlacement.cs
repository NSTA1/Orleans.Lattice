using Orleans;
using Orleans.Lattice;
using static VehicleFleetSimulator.AzureThroughput.Engine.BenchExceptionHelpers;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Spreads a freshly warmed tree's WAL partitions across the keyed
/// multi-account WAL providers (<c>acct1</c>, <c>acct2</c>, ...) so that
/// <c>BENCH_WAL_ACCOUNTS</c> is the independent variable of a cohort.
/// </summary>
/// <remarks>
/// Shared by both hosts. The single-VM silo spreads before its TCP listener
/// opens; on Layer 3 the silo returns before that step in cluster ingest mode,
/// so the Orleans-client producer spreads after its warm-up instead. Partition
/// <c>p</c> routes to account <c>p % accounts</c>; index 0 stays on the
/// default provider. The tree is warmed and empty at this point, so each move
/// is a placement-pin flip with no retained tail to copy.
/// </remarks>
internal static class BenchWalPlacement
{
    // Source of truth: Orleans.Lattice LatticeConstants.AdminGrainKey (internal
    // to the library; the bench assemblies have no InternalsVisibleTo grant).
    private const string AdminGrainKey = "_lattice_admin";

    /// <summary>
    /// The moves that spread <paramref name="walPartitions"/> partitions
    /// round-robin over <paramref name="walAccounts"/> accounts. Partitions
    /// that map to account 0 stay on the default provider and are omitted.
    /// </summary>
    public static IReadOnlyList<(int Partition, string TargetProviderKey)> PlanMoves(int walPartitions, int walAccounts)
    {
        var moves = new List<(int Partition, string TargetProviderKey)>();
        if (walAccounts <= 1)
        {
            return moves;
        }

        for (var p = 0; p < walPartitions; p++)
        {
            var accountIndex = p % walAccounts;
            if (accountIndex != 0)
            {
                moves.Add((p, $"acct{accountIndex}"));
            }
        }

        return moves;
    }

    /// <summary>
    /// Executes the spread and logs the resulting placement as
    /// <c>{logPrefix} wal-placement treeId=.. accounts=N partitions=M version=.. -&gt; ...</c>.
    /// A failed move logs <c>{logPrefix} ERROR wal-placement-spread</c> and
    /// rethrows, so the cohort never runs on a placement it did not ask for. A
    /// failed read-back only warns: the moves themselves succeeded.
    /// </summary>
    public static async Task SpreadAsync(
        IGrainFactory grainFactory,
        string treeId,
        int walPartitions,
        int walAccounts,
        string logPrefix,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        if (walAccounts <= 1)
        {
            return;
        }

        var admin = grainFactory.GetGrain<ILatticeAdmin>(AdminGrainKey);
        var moves = PlanMoves(walPartitions, walAccounts);
        if (moves.Count > 0)
        {
            try
            {
                await admin.ExecuteWalMoveAsync(treeId, moves, null, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"{logPrefix} ERROR wal-placement-spread treeId={treeId} accounts={walAccounts} partitions={walPartitions} moves={moves.Count} FAILED: {ex.GetType().Name}: {Truncate(ex.Message, 240)}");
                throw;
            }
        }

        try
        {
            var placement = await admin.GetWalPlacementAsync(treeId, cancellationToken).ConfigureAwait(false);
            var summary = string.Join(",", placement.Partitions.Select(pp => $"{pp.Partition}:{pp.ProviderKey}"));
            Console.WriteLine($"{logPrefix} wal-placement treeId={treeId} accounts={walAccounts} partitions={walPartitions} version={placement.Version} -> {summary}");
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            Console.WriteLine($"{logPrefix} WARN wal-placement read treeId={treeId} failed: {ex.GetType().Name}: {Truncate(ex.Message, 160)}");
        }
    }
}
