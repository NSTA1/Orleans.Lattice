using System.Threading.Channels;

namespace VehicleFleetSimulator.AzureThroughput.Producer;

// Transfer chunks across the multi-writer channel so its shared lock is not
// acquired once per key. The engine still consumes the same individual entries.
internal sealed class GeneratorChannelReader(ChannelReader<KeyValuePair<string, byte[]>[]> source)
    : ChannelReader<KeyValuePair<string, byte[]>>
{
    private KeyValuePair<string, byte[]>[] current = [];
    private int offset;

    public override bool TryRead(out KeyValuePair<string, byte[]> item)
    {
        if (offset == current.Length)
        {
            if (!source.TryRead(out var chunk))
            {
                item = default;
                return false;
            }
            current = chunk;
            offset = 0;
        }
        item = current[offset++];
        if (offset == current.Length)
        {
            current = [];
            offset = 0;
        }
        return true;
    }

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        => offset < current.Length ? new(true) : source.WaitToReadAsync(cancellationToken);
}
