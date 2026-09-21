namespace VehicleFleetSimulator.AzureThroughput.Engine;

internal sealed record IngestSettings(string TreeId, int TcpPort, int BatchSize, TimeSpan FlushInterval, TimeSpan ReportInterval, int FlushConcurrency, int ShardCountOverride, BenchWorkloadMode WorkloadMode, int AtomicBatchSize, int PreseedKeyCount, int WalMaxPendingBatches, int ResponseTimeoutSec, int WalPartitions, int WalAccounts);
