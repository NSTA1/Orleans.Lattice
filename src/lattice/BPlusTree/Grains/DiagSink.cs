#if LATTICE_DIAG
namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// File-based diagnostic event sink. Routes diagnostic events emitted
/// during chaos and reshard investigations to a known path so they
/// survive Orleans' in-process silo stdout capture (which NUnit does
/// not surface through the trx file).
/// <para>
/// Compiled only when the <c>LATTICE_DIAG</c> symbol is defined (see
/// <c>Orleans.Lattice.csproj</c>, opt-in via <c>/p:LatticeDiag=true</c>).
/// Call sites must themselves be guarded with <c>#if LATTICE_DIAG</c>.
/// </para>
/// </summary>
internal static class DiagSink
{
    private static readonly object Gate = new();
    private static readonly string Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "lattice-diag.log");
    private static readonly DateTime ProcessStart = DateTime.UtcNow;

    public static void Write(string message)
    {
        var ts = (DateTime.UtcNow - ProcessStart).TotalMilliseconds;
        var line = $"{ts:F3}ms {message}";
        lock (Gate)
        {
            try { System.IO.File.AppendAllText(Path, line + System.Environment.NewLine); }
            catch { /* best-effort */ }
        }
    }

    public static void Reset()
    {
        lock (Gate)
        {
            try { if (System.IO.File.Exists(Path)) System.IO.File.Delete(Path); } catch { }
        }
    }

    public static string LogPath => Path;

    public static int DecodeRound(byte[]? value)
    {
        if (value is null || value.Length < 6) return -1;
        if (value[0] != (byte)'v' || value[1] != (byte)'-') return -1;
        int round = 0;
        for (int i = 2; i < value.Length && i < 5; i++)
        {
            var c = value[i];
            if (c < (byte)'0' || c > (byte)'9') return -1;
            round = round * 10 + (c - (byte)'0');
        }
        return round;
    }
}
#endif
