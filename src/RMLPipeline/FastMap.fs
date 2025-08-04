namespace RMLPipeline

[<RequireQualifiedAccess>]
module FastMap =

    open FSharp.HashCollections
    open System.Collections.Generic

    // type defs
    [<AutoOpen>] 
    module Types = 
        type FastMap<'Key, 'Value> = FSharp.HashCollections.HashMap<'Key, 'Value>
        type FastComparatorMap<'Key, 'Value, 'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                                            'Comparer : (new : unit -> 'Comparer)> =
            FSharp.HashCollections.HashMap<'Key, 'Value, 'Comparer>

    type private Pair<'k, 'v> = Pair of 'k * 'v
    
    // wrappers 

    let empty<'Key, 'Value when 'Key : equality> : HashMap<'Key,'Value> = HashMap.empty
    let inline add (key: 'Key) (value: 'Value) (map: FastMap<'Key, 'Value>) : FastMap<'Key, 'Value> =
        HashMap.add key value map
    let inline tryFind (key: 'Key) (map: FastMap<'Key, 'Value>) : 'Value voption =
        HashMap.tryFind key map
    let inline remove (key: 'Key) (map: FastMap<'Key, 'Value>) : FastMap<'Key, 'Value> =
        HashMap.remove key map
    let inline containsKey (key: 'Key) (map: FastMap<'Key, 'Value>) : bool =
        HashMap.containsKey key map
    let inline toSeq (map: FastMap<'Key, 'Value>) : struct ('Key * 'Value) seq =
        HashMap.toSeq map
    let inline ofSeq (seq: seq<KeyValuePair<'Key, 'Value>>) : FastMap<'Key, 'Value> =
        HashMap.ofSeq seq
    let inline count (map: FastMap<'Key, 'Value>) : int =
        HashMap.count map
    let inline isEmpty (map: FastMap<'Key, 'Value>) : bool =
        HashMap.isEmpty map
    let inline keys (map: FastMap<'Key, 'Value>) : seq<'Key> =
        HashMap.keys map
    let inline values (map: FastMap<'Key, 'Value>) : seq<'Value> =
        HashMap.values map
    let fold<'Key, 'Value, 'State> 
            (folder: 'State -> 'Key -> 'Value -> 'State)
            (initialState: 'State)
            (table: FastMap<'Key, 'Value>) : 'State = 
        // TODO: make this inline-able
        let seq' : Pair<'Key, 'Value> seq = HashMap.toSeq table |> Seq.map (fun struct (k, v) -> Pair(k, v))
        seq' |> Seq.fold (fun (s: 'State) (Pair(k, v)) -> folder s k v) initialState

    let iter<'Key, 'Value> 
            (action: 'Key -> 'Value -> unit)
            (table: FastMap<'Key, 'Value>) : unit =
        HashMap.toSeq table |> Seq.iter (fun struct (k, v) -> action k v)

    module ComparatorMap =
        let empty<'Key, 'Value, 
                    'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                   'Comparer : (new : unit -> 'Comparer)> 
                                   : FastComparatorMap<'Key, 'Value, 'Comparer> =
            HashMap.emptyWithComparer<'Key, 'Value, 'Comparer>

        let inline add<'Key, 'Value, 
                        'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                       'Comparer : (new : unit -> 'Comparer)> 
                        (key: 'Key) (value: 'Value) 
                        (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : FastComparatorMap<'Key, 'Value, 'Comparer> = 
            HashMap.add key value map

        let inline tryFind<'Key, 'Value, 
                            'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                           'Comparer : (new : unit -> 'Comparer)> 
                (key: 'Key) 
                (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : 'Value voption =
            HashMap.tryFind key map

        let inline remove<'Key, 'Value, 
                            'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                           'Comparer : (new : unit -> 'Comparer)> 
                (key: 'Key) 
                (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : FastComparatorMap<'Key, 'Value, 'Comparer> =
            HashMap.remove key map
        let inline containsKey<'Key, 'Value, 
                                'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                               'Comparer : (new : unit -> 'Comparer)>
                (key: 'Key) 
                (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : bool =
            HashMap.containsKey key map        
        let inline toSeq<'Key, 'Value, 
                            'Comparer when 'Comparer :> IEqualityComparer<'Key> and 
                                           'Comparer : (new : unit -> 'Comparer)> 
                (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : struct ('Key * 'Value) seq =
            HashMap.toSeq map        
        let inline count (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : int =
            HashMap.count map
        let inline isEmpty (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : bool =
            HashMap.isEmpty map
        let inline keys (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : seq<'Key> =
            HashMap.keys map
        let inline values (map: FastComparatorMap<'Key, 'Value, 'Comparer>) : seq<'Value> =
            HashMap.values map


