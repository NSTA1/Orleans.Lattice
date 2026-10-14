using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Schema.Tests.Chaos;

internal static class SchemaOperationStatusPoll
{
    internal static async Task<LatticeOperationRecord> WaitForFailedAsync(
        Func<Task<LatticeOperationRecord?>> read,
        TimeSpan? timeout = null,
        TimeSpan? cadence = null)
    {
        LatticeOperationRecord? failed = null;
        await TestPoll.UntilAsync(
            async () =>
            {
                LatticeOperationRecord? observed;
                try
                {
                    observed = await read();
                }
                catch (TimeoutException ex)
                {
                    // Only the lost-silo status read is retryable, not the remediation work.
                    TestContext.Progress.WriteLine($"Lost-silo status read timed out; retrying within the poll deadline: {ex.Message}");
                    return false;
                }

                if (observed is not { State: LatticeOperationState.Failed })
                {
                    return false;
                }

                failed = observed;
                return true;
            },
            "the remediation of the killed runner to read as failed rather than running",
            timeout: timeout ?? TimeSpan.FromMinutes(2),
            cadence: cadence ?? TimeSpan.FromMilliseconds(250));
        return failed ?? throw new InvalidOperationException("The status barrier completed without a failed record.");
    }
}
