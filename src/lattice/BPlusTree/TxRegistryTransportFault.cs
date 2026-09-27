using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Classifies a failure of a call to the per-tree <see cref="ITxRegistryGrain"/>
/// on a read path (issues #2215 and #3641). Only a <b>transport</b> failure - the
/// registry could not be reached or did not answer in time - is translated into
/// <see cref="LatticeTransactionOutcomeUnavailableException"/> or, on a multi-key
/// read, into an "unverifiable" stability verdict. Anything else is a genuine
/// fault and propagates unchanged, and cooperative cancellation is never
/// swallowed.
/// </summary>
internal static class TxRegistryTransportFault
{
    /// <summary>
    /// True when <paramref name="exception"/> is a registry transport failure: a
    /// <see cref="TimeoutException"/> (the Orleans response timeout) or an
    /// <see cref="OrleansException"/> (which covers an unavailable silo and a
    /// rejected message). A cancellation and a Lattice domain refusal
    /// (<see cref="ILatticeDomainFault"/>, including an already-translated
    /// <see cref="LatticeTransactionOutcomeUnavailableException"/>) are not.
    /// </summary>
    public static bool IsTransportFailure(Exception exception) =>
        exception is not OperationCanceledException
        && exception is not ILatticeDomainFault
        && (exception is TimeoutException || exception is OrleansException);
}
