namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The ONNX Runtime execution provider the embedding server runs its session on.
/// Selected once at startup from <c>EMBED_PROVIDER</c>; an unrecognised value
/// resolves to <see cref="Cpu"/> rather than failing, so a misconfigured
/// accelerator degrades to a working server instead of a dead one.
/// </summary>
internal enum EmbedExecutionProvider
{
    /// <summary>The portable CPU provider. Always available, and the fallback.</summary>
    Cpu,

    /// <summary>
    /// The CUDA provider, for an NVIDIA GPU. Only present in an image built with
    /// the <c>cuda</c> ONNX Runtime flavour.
    /// </summary>
    Cuda,

    /// <summary>
    /// The DirectML provider, for any DirectX 12 GPU (including AMD and Intel
    /// integrated GPUs). Windows-only, so it is never available in the Linux
    /// container image; it exists for running this server natively on Windows.
    /// </summary>
    DirectML,
}
