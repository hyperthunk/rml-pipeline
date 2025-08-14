namespace RMLPipeline.Tests

open FsCheck
open RMLPipeline
open RMLPipeline.Model
open RMLPipeline.DSL
open System

(*
  SharedGenerators:
  Centralises all FsCheck generators so RuntimePlannerTests and PlannerModelTests
  can reuse identical data distributions (important for correlation between
  base planner and runtime planner metadata).

  NOTE:
  - The names exported here mirror those previously declared inside
    PlannerModelTests. To avoid breaking existing tests immediately, you can
    (temporary) keep the old definitions and gradually migrate, or remove them
    and replace with `open RMLPipeline.Tests.SharedGenerators`.
*)

module SharedGenerators =

    // -------- Primitive Helpers ------------------------------------------------
    let private chooseNonEmptyString =
        Gen.elements [ "id"; "name"; "type"; "category"; "parentId"; "otherKey"; "otherId"; "pName"; "cid"; "sid" ]

    // -------- Triples Map Generators ------------------------------------------
    let genBasicTriplesMap =
        gen {
            let! id = Gen.choose(1, 5000)
            let! addType = Gen.frequency [(7, Gen.constant true); (3, Gen.constant false)]
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator $"$.items[{id}]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI $"http://example.org/item/{id}/{{id}}")
                    if addType then do! addClass "http://example.org/Item"
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/name"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    })
                })
                if addType then
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                        do! addObject (URI "http://example.org/Item")
                    })
            })
        }

    let genTriplesMapWithJoin =
        gen {
            let parent = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.parents[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/parent/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/pName"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "pName")
                    })
                })
            })
            let child = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.children[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/child/{cid}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/hasParent"
                    // multi-key join 50% chance
                    let! multi = Gen.frequency [(5, Gen.constant true); (5, Gen.constant false)]
                    do! addRefObjectMap parent (refObjectMap {
                        do! addJoinCondition (join {
                            do! child "parentId"
                            do! parent "id"
                        })
                        if multi then
                            do! addJoinCondition (join {
                                do! child "otherKey"
                                do! parent "otherId"
                            })
                    })
                })
            })
            return [| parent; child |]
        }

    let genWideTriplesMap =
        gen {
            let! count = Gen.choose(65, 90)
            let predicateNames =
                [ for i in 0 .. count - 1 -> $"http://example.org/p{i}" ]
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.wide[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/wide/{sid}")
                })
                for p in predicateNames do
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate p
                        do! addObjectMap (objectMap {
                            do! objectTermMap (templateTermAsLiteral $"{{f_{p.GetHashCode() &&& 0xFFFF}}}")
                        })
                    })
            })
        }

    let genTriplesMapSet =
        gen {
            let! variant = Gen.choose(1, 5)
            match variant with
            | 1 ->
                let! tm = genBasicTriplesMap
                return [| tm |]
            | 2 ->
                let! arr = genTriplesMapWithJoin
                return arr
            | 3 ->
                let! wide = genWideTriplesMap
                return [| wide |]
            | 4 ->
                let! n = Gen.choose(2, 5)
                let! maps = Gen.listOfLength n genBasicTriplesMap
                return maps |> List.toArray
            | _ ->
                let! a = genTriplesMapWithJoin
                let! extra = Gen.listOfLength 2 genBasicTriplesMap
                return Array.append a (extra |> List.toArray)
        }

    // -------- Planner Config ---------------------------------------------------
    let genPlannerConfig =
        gen {
            let! mode = Gen.elements [LowMemory; Balanced; HighPerformance]
            let! chunk =
                match mode with
                | LowMemory -> Gen.choose(5, 100)
                | HighPerformance -> Gen.choose(100, 600)
                | Balanced -> Gen.choose(25, 400)
            let! idx = Gen.elements [None; Some NoIndex; Some HashIndex; Some FullIndex]
            let! maxMem = Gen.elements [None; Some 64; Some 128; Some 256; Some 512; Some 1024]
            return {
                MemoryMode = mode
                MaxMemoryMB = maxMem
                ChunkSize = chunk
                IndexStrategy = idx
                EnableStringPooling = true
                MaxLocalStrings = Some 2000
                MaxGroupPools = Some 25
            }
        }

    // Combined generator for convenience
    let genPlanInput =
        gen {
            let! maps = genTriplesMapSet
            let! cfg = genPlannerConfig
            return maps, cfg
        }