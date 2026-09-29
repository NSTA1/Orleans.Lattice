using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// Records every slice timing a build reports, so a fixture can assert the
/// observer seam is actually driven rather than merely present.
/// </summary>
internal sealed class RecordingBuildObserver : IVectorIndexBuildObserver
{
    private readonly List<VectorIndexBuildSliceTimings> _slices = [];

    /// <summary>Every slice reported, in order.</summary>
    internal IReadOnlyList<VectorIndexBuildSliceTimings> Slices => _slices;

    /// <summary>The total items the reported slices consumed.</summary>
    internal int TotalConsumed
    {
        get
        {
            var total = 0;
            for (var i = 0; i < _slices.Count; i++)
            {
                total += _slices[i].Consumed;
            }

            return total;
        }
    }

    public void OnSliceCompleted(in VectorIndexBuildSliceTimings timings) => _slices.Add(timings);
}
