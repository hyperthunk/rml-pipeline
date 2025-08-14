namespace RMLPipeline.Internal

open System
open RMLPipeline.Internal.RuntimePlanner

module RuntimePlannerDiagnostics =

    type MapDiagnostics = {
        MapIndex: int
        ReferenceCount: int
        JoinDescriptorCount: int
        LargeMask: bool
        TupleCount: int
        JoinParents: int list
        JoinKeysPerDescriptor: (int * int) list  // (parentKeys, childKeys)
    }

    type RuntimePlanDiagnostics = {
        MapCount: int
        TotalReferences: int
        TotalJoinDescriptors: int
        Maps: MapDiagnostics[]
        PathRefIndexEntries: int
    }

    let summarize (rt: RuntimePlan) =
        let mapsDiag =
            rt.Maps
            |> Array.map (fun m ->
                {
                    MapIndex = m.Base.Index
                    ReferenceCount = m.ReferenceInfos.Length
                    JoinDescriptorCount = m.JoinDescriptors.Length
                    LargeMask = not m.SupportsBitset64
                    TupleCount = m.TupleRequirements.Length
                    JoinParents = m.JoinDescriptors |> Array.map (fun jd -> jd.ParentMapIndex) |> Array.distinct |> Array.toList
                    JoinKeysPerDescriptor =
                        m.JoinDescriptors
                        |> Array.map (fun jd -> jd.ParentKeyRefIndices.Length, jd.ChildKeyRefIndices.Length)
                        |> Array.toList
                })
        {
            MapCount = rt.Maps.Length
            TotalReferences = rt.Maps |> Array.sumBy (fun m -> m.ReferenceInfos.Length)
            TotalJoinDescriptors = rt.Maps |> Array.sumBy (fun m -> m.JoinDescriptors.Length)
            Maps = mapsDiag
            PathRefIndexEntries = rt.PathRefIndex |> FastMap.count
        }

    let diagnosticsToString (d: RuntimePlanDiagnostics) =
        let sb = System.Text.StringBuilder()
        sb.AppendLine($"RuntimePlan: maps={d.MapCount} totalRefs={d.TotalReferences} totalJoinDesc={d.TotalJoinDescriptors} pathIdx={d.PathRefIndexEntries}")
          |> ignore
        for m in d.Maps do
            sb.AppendLine(
                $" Map[{m.MapIndex}] refs={m.ReferenceCount} tuples={m.TupleCount} joinDesc={m.JoinDescriptorCount} largeMask={m.LargeMask} parents={String.Join(',', m.JoinParents)} keys={String.Join(';', m.JoinKeysPerDescriptor |> List.map (fun (p,c) -> $"{p}->{c}"))}"
            ) |> ignore
        sb.ToString()