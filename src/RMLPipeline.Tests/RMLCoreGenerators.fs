namespace RMLPipeline.Tests

/// Non-recursive generators for RML core types designed to stress-test 
/// the string pool and planner without causing FsCheck stack overflow issues
module RMLCoreGenerators =

    open FsCheck
    open RMLPipeline.Model
    open RMLPipeline.DSL
    open RMLPipeline.Core
    open System

    let genIdentifier = 
        Gen.oneof [
            Gen.elements ["id"; "employeeId"; "personId"; "departmentId"; "userId"; "customerId"; "orderId"]
            Gen.choose(1, 999) |> Gen.map (sprintf "item_%d")
            Gen.choose(1, 50) |> Gen.map (sprintf "entity_%d")
        ]

    let genNestedJSONPath = 
        let genBasePath = Gen.elements ["$"; "$.data"; "$.root"; "$.items"; "$.entities"]
        let genArrayAccess = Gen.oneof [
            Gen.constant "[*]"
            Gen.choose(0, 10) |> Gen.map (sprintf "[%d]")
            Gen.elements ["[0]"; "[last()]"; "[-1]"]
        ]
        let genProperty = Gen.oneof [
            genIdentifier
            Gen.elements ["name"; "value"; "type"; "category"; "status"; "metadata"; "attributes"]
            Gen.choose(1, 5) |> Gen.map (sprintf "prop%d")
        ]
        
        gen {
            let! basePath = genBasePath
            let! depth = Gen.choose(1, 4)
            let! segments = Gen.listOfLength depth (Gen.oneof [
                genArrayAccess
                genProperty |> Gen.map (sprintf ".%s")
                genArrayAccess |> Gen.two |> Gen.map (fun (a, b) -> a + "." + b.TrimStart('[').TrimEnd(']'))
            ])
            return segments |> List.fold (+) basePath
        }

    let genComplexJSONPath =
        Gen.oneof [
            genNestedJSONPath
            // Complex JSONPath expressions that will stress string interning
            Gen.elements [
                "$.employees[*].departments[*].projects[*]"
                "$.organizations[?(@.type=='company')].divisions[*]"
                "$.data.relationships[*].metadata.annotations[*]"
                "$.entities[*].properties[?(@.required==true)]"
                "$.catalog.categories[*].items[*].variants[*]"
                "$.users[*].permissions[*].resources[*]"
                "$.workflows[*].steps[*].conditions[*]"
                "$.api.endpoints[*].parameters[*].validation"
                "$.schema.definitions[*].properties[*].oneOf[*]"
                "$.configurations[*].environments[*].variables[*]"
            ]
        ]

    let genNamespace =
        Gen.oneof [
            Gen.elements [
                "http://example.org"
                "http://company.internal"
                "http://data.gov"
                "http://api.service.com"
                "http://schema.enterprise.org"
                "http://vocab.linked-data.org"
                "http://ontology.domain.edu"
                "http://rdf.research.institute"
            ]
            Gen.choose(1, 20) |> Gen.map (sprintf "http://org%d.example.com")
        ]

    let genResourcePath =
        gen {
            let! resourceType = 
                Gen.elements 
                    ["person"; 
                    "employee"; 
                    "department"; 
                    "project"; 
                    "order"; 
                    "customer"; 
                    "product"; 
                    "service"]
            let! hasSubPath = 
                Gen.frequency <| Seq.ofList [7, Gen.constant true; 3, Gen.constant false]
            if hasSubPath then
                let! subPath = Gen.elements ["profile"; "details"; "metadata"; "relationships"; "history"; "analytics"]
                return sprintf "%s/{id}/%s" resourceType subPath
            else
                return sprintf "%s/{id}" resourceType
        }

    let genComplexTemplate =
        gen {
            let! ns = genNamespace
            let! resourcePath = genResourcePath
            let! hasFunctionCall = 
                Gen.frequency <| Seq.ofList [2, Gen.constant true; 8, Gen.constant false]
            if hasFunctionCall then
                let! funcName = Gen.elements ["encode"; "hash"; "normalize"; "transform"; "validate"]
                let! param = genIdentifier
                return sprintf "%s/%s/{%s(%s)}" ns resourcePath funcName param
            else
                return sprintf "%s/%s" ns resourcePath
        }

    let genReference =
        Gen.oneof [
            genIdentifier
            Gen.elements ["name"; "email"; "title"; "description"; "value"; "category"; "type"; "status"]
            Gen.choose(1, 10) |> Gen.map (sprintf "field_%d")
            // Nested references that will create more strings
            gen {
                let! base' = genIdentifier
                let! suffix = Gen.elements ["_normalized"; "_encoded"; "_validated"; "_processed"]
                return base' + suffix
            }
        ]

    let genClassIRI =
        Gen.oneof [
            Gen.elements [
                "http://xmlns.com/foaf/0.1/Person"
                "http://xmlns.com/foaf/0.1/Organization"
                "http://schema.org/Person"
                "http://schema.org/Organization"
                "http://schema.org/Product"
                "http://schema.org/Service"
                "http://purl.org/dc/terms/Agent"
                "http://www.w3.org/ns/org#Organization"
            ]
            gen {
                let! ns = genNamespace
                let! className = 
                    Gen.elements 
                        [
                            "Person"; 
                            "Employee"; 
                            "Department"; 
                            "Project"; 
                            "Customer"; 
                            "Order"; 
                            "Product"
                            "Service"; 
                            "Role"; 
                            "Task"; 
                            "Event"; 
                            "Resource"; 
                            "Entity"; 
                            "Asset"; 
                            "Transaction"; 
                            "Interaction";
                        ]
                return sprintf "%s/vocab#%s" ns className
            }
        ]

    let genPredicateIRI =
        Gen.oneof [
            Gen.elements [
                "http://xmlns.com/foaf/0.1/name"
                "http://xmlns.com/foaf/0.1/email"
                "http://xmlns.com/foaf/0.1/knows"
                "http://schema.org/name"
                "http://schema.org/description"
                "http://schema.org/memberOf"
                "http://purl.org/dc/terms/title"
                "http://purl.org/dc/terms/creator"
                "http://www.w3.org/2000/01/rdf-schema#label"
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            ]
            gen {
                let! ns = genNamespace
                let! propName = 
                    Gen.elements 
                        [
                            "hasRole"; 
                            "belongsTo"; 
                            "isAssignedTo"; 
                            "manages"; 
                            "reportsTo"; 
                            "owns"; 
                            "uses"; 
                            "contains"; 
                            "isPartOf"; 
                            "isRelatedTo"; 
                            "hasStatus"; 
                            "hasCategory"; 
                            "hasPriority"; 
                            "hasType"; 
                            "hasValue"; 
                            "hasDescription";
                        ]
                return sprintf "%s/property#%s" ns propName
            }
        ]

    let genDatatype =
        Gen.elements [
            "http://www.w3.org/2001/XMLSchema#string"
            "http://www.w3.org/2001/XMLSchema#integer"
            "http://www.w3.org/2001/XMLSchema#decimal"
            "http://www.w3.org/2001/XMLSchema#boolean"
            "http://www.w3.org/2001/XMLSchema#dateTime"
            "http://www.w3.org/2001/XMLSchema#date"
            "http://www.w3.org/2001/XMLSchema#time"
            "http://www.w3.org/2001/XMLSchema#anyURI"
            "http://www.w3.org/2001/XMLSchema#base64Binary"
        ]

    let genExpressionMap =
        Gen.oneof [
            // Template-based expression
            genComplexTemplate |> Gen.map (fun t -> { 
                Template = Some t
                Reference = None
                Constant = None
                FunctionExecution = None 
            })
            
            // Reference-based expression
            genReference |> Gen.map (fun r -> { 
                Template = None
                Reference = Some r
                Constant = None
                FunctionExecution = None 
            })
            
            // Constant expressions
            gen {
                let! constType = Gen.choose(1, 3)
                let! value = 
                    match constType with
                    | 1 -> genNamespace |> Gen.map URI
                    | 2 -> 
                        Gen.elements 
                            [
                                "Active"; 
                                "Inactive"; 
                                "Pending"; 
                                "Completed"; 
                                "Draft"
                            ] |> Gen.map Literal
                    | _ -> genIdentifier |> Gen.map BlankNode
                return { 
                    Template = None
                    Reference = None
                    Constant = Some value
                    FunctionExecution = None 
                }
            }
            
            // Complex template with multiple placeholders
            gen {
                let! ns = genNamespace
                let! id1 = genIdentifier
                let! id2 = genIdentifier
                let! separator = Gen.elements ["/"; "-"; "_"; ":"]
                return { 
                    Template = Some (sprintf "%s/{%s}%s{%s}" ns id1 separator id2)
                    Reference = None
                    Constant = None
                    FunctionExecution = None 
                }
            }
        ]

    let genTermMap =
        gen {
            let! exprMap = genExpressionMap
            let! termType = Gen.frequency [
                4, Gen.constant (Some IRITerm)
                3, Gen.constant (Some LiteralTerm)
                2, Gen.constant (Some BlankNodeTerm)
                1, Gen.constant (Some URITerm)
            ]
            return {
                ExpressionMap = exprMap
                TermType = termType
                LogicalTarget = None
            }
        }

    let genLogicalSource =
        gen {
            let! hasIterator = 
                Gen.frequency [(8, Gen.constant true); (2, Gen.constant false)]
            let! iterator = if hasIterator then genComplexJSONPath |> Gen.map Some else Gen.constant None
            
            let! refForm = Gen.frequency [
                6, Gen.constant (Some JSONPath)
                2, Gen.constant (Some XPath)
                1, Gen.constant (Some CSV)
                1, Gen.constant None
            ]
            
            return { 
                SourceIterator = iterator
                SourceReferenceFormulation = refForm 
            }
        }

    let genObjectMap =
        gen {
            let! termMap = genTermMap
            let! hasDatatype = 
                Gen.frequency [3, Gen.constant true; 7, Gen.constant false]
            let! datatype = if hasDatatype then genDatatype |> Gen.map Some else Gen.constant None

            let! hasLanguage = 
                Gen.frequency [1, Gen.constant true; 9, Gen.constant false]
            let! language = 
                if hasLanguage then 
                    Gen.elements ["en"; "es"; "fr"; "de"; "it"] |> Gen.map Some 
                else Gen.constant None
            
            return {
                ObjectTermMap = termMap
                Datatype = datatype
                DatatypeMap = None
                Language = language
                LanguageMap = None
            }
        }

    let genPredicateObjectMap =
        gen {
            let! predicateCount = Gen.choose(1, 3)
            let! predicates = Gen.listOfLength predicateCount genPredicateIRI
            
            let! mapType = Gen.choose(1, 3)
            match mapType with
            | 1 -> 
                // Direct object values
                let! objectCount = Gen.choose(1, 2)
                let! objects = Gen.listOfLength objectCount (gen {
                    let! objType = Gen.choose(1, 3)
                    match objType with
                    | 1 -> return! genNamespace |> Gen.map URI
                    | 2 -> return! Gen.elements ["High"; "Medium"; "Low"; "Critical"; "Normal"] |> Gen.map Literal
                    | _ -> return! genIdentifier |> Gen.map BlankNode
                })
                return {
                    Predicate = predicates
                    PredicateMap = []
                    Object = objects
                    ObjectMap = []
                    RefObjectMap = []
                    GraphMap = []
                }
            | 2 ->
                // Object maps
                let! objectMapCount = Gen.choose(1, 2)
                let! objectMaps = Gen.listOfLength objectMapCount genObjectMap
                return {
                    Predicate = predicates
                    PredicateMap = []
                    Object = []
                    ObjectMap = objectMaps
                    RefObjectMap = []
                    GraphMap = []
                }
            | _ ->
                // Mixed approach
                let! hasObjects = 
                    Gen.frequency [6, Gen.constant true; 4, Gen.constant false]
                let! objects = 
                    if hasObjects then 
                        gen {
                            let! count = Gen.choose(1, 2)
                            return! Gen.listOfLength count (genIdentifier |> Gen.map Literal)
                        }
                    else Gen.constant []
                
                let! hasObjectMaps = 
                    Gen.frequency [4, Gen.constant true; 6, Gen.constant false]
                let! objectMaps = 
                    if hasObjectMaps then 
                        Gen.listOfLength 1 genObjectMap 
                    else Gen.constant []
                
                return {
                    Predicate = predicates
                    PredicateMap = []
                    Object = objects
                    ObjectMap = objectMaps
                    RefObjectMap = []
                    GraphMap = []
                }
        }

    let genSubjectMap =
        gen {
            let! subjectTermMap = genTermMap
            let! classCount = Gen.choose(0, 3)
            let! classes = Gen.listOfLength classCount genClassIRI
            
            return {
                SubjectTermMap = subjectTermMap
                Class = classes
                GraphMap = []
            }
        }

    let genJoin =
        gen {
            let! child = genReference |> Gen.map Some
            let! parent = genReference |> Gen.map Some
            return {
                Child = child
                Parent = parent
                ChildMap = None
                ParentMap = None
            }
        }

    let genBasicTriplesMap =
        gen {
            let! logicalSource = genLogicalSource
            let! subjectMap = genSubjectMap
            let! pomCount = Gen.choose(1, 4)
            let! predicateObjectMaps = Gen.listOfLength pomCount genPredicateObjectMap
            let! hasBaseIRI = 
                Gen.frequency [3, Gen.constant true; 7, Gen.constant false]
            let! baseIRI = if hasBaseIRI then genNamespace |> Gen.map Some else Gen.constant None
            
            return {
                LogicalSource = logicalSource
                SubjectMap = Some subjectMap
                Subject = None
                PredicateObjectMap = predicateObjectMaps
                BaseIRI = baseIRI
                LogicalTarget = None
            }
        }

    let genTriplesMapWithJoin =
        gen {
            // Create a parent triples map
            let! parentLogicalSource = genLogicalSource
            let! parentSubjectMap = genSubjectMap
            let! parentPOM = genPredicateObjectMap
            
            let parentTriplesMap = {
                LogicalSource = parentLogicalSource
                SubjectMap = Some parentSubjectMap
                Subject = None
                PredicateObjectMap = [parentPOM]
                BaseIRI = None
                LogicalTarget = None
            }
            
            // Create child triples map with reference object map
            let! childLogicalSource = genLogicalSource
            let! childSubjectMap = genSubjectMap
            let! regularPOMCount = Gen.choose(1, 2)
            let! regularPOMs = Gen.listOfLength regularPOMCount genPredicateObjectMap
            
            // Create join condition
            let! joinCondition = genJoin
            let! joinPredicate = genPredicateIRI
            
            let refObjectMap = {
                ParentTriplesMap = parentTriplesMap
                JoinCondition = [joinCondition]
            }
            
            let joinPOM = {
                Predicate = [joinPredicate]
                PredicateMap = []
                Object = []
                ObjectMap = []
                RefObjectMap = [refObjectMap]
                GraphMap = []
            }
            
            return {
                LogicalSource = childLogicalSource
                SubjectMap = Some childSubjectMap
                Subject = None
                PredicateObjectMap = joinPOM :: regularPOMs
                BaseIRI = None
                LogicalTarget = None
            }
        }

    let genComplexTriplesMap =
        Gen.oneof [
            genBasicTriplesMap
            genTriplesMapWithJoin
            // Even more complex variant with multiple joins
            gen {
                let! baseMap = genTriplesMapWithJoin
                let! extraPOMCount = Gen.choose(1, 2)
                let! extraPOMs = Gen.listOfLength extraPOMCount genPredicateObjectMap
                return { baseMap with PredicateObjectMap = baseMap.PredicateObjectMap @ extraPOMs }
            }
        ]

    let genStringPoolStressTriplesMap =
        gen {
            let! complexity = Gen.choose(1, 3)
            match complexity with
            | 1 -> return! genBasicTriplesMap
            | 2 -> return! genTriplesMapWithJoin
            | _ -> 
                // Maximum complexity variant
                let! logicalSource = gen {
                    let! deepPath = gen {
                        let! segments = Gen.listOfLength 6 (Gen.oneof [
                            genIdentifier
                            Gen.elements ["items"; "data"; "records"; "entities"; "objects"]
                            Gen.choose(1, 100) |> Gen.map string
                        ])
                        return "$.root." + String.Join(".", segments) + "[*]"
                    }
                    return { SourceIterator = Some deepPath; SourceReferenceFormulation = Some JSONPath }
                }
                
                let! subjectTemplate = gen {
                    let! baseNS = genNamespace
                    let! segments = Gen.listOfLength 4 genIdentifier
                    return sprintf "%s/%s" baseNS (String.Join("/", segments |> List.map (sprintf "{%s}")))
                }
                
                let subjectMap = {
                    SubjectTermMap = {
                        ExpressionMap = { Template = Some subjectTemplate; Reference = None; Constant = None; FunctionExecution = None }
                        TermType = Some IRITerm
                        LogicalTarget = None
                    }
                    Class = []
                    GraphMap = []
                }
                
                let! pomCount = Gen.choose(3, 6)
                let! poms = Gen.listOfLength pomCount genPredicateObjectMap
                
                return {
                    LogicalSource = logicalSource
                    SubjectMap = Some subjectMap
                    Subject = None
                    PredicateObjectMap = poms
                    BaseIRI = Some "http://stress.test.example.org/"
                    LogicalTarget = None
                }
        }

    let genTriplesMapArray =
        gen {
            let! arraySize = Gen.choose(1, 8)
            let! complexity = Gen.choose(1, 3)
            match complexity with
            | 1 -> return! Gen.arrayOfLength arraySize genBasicTriplesMap
            | 2 -> return! Gen.arrayOfLength arraySize (Gen.oneof [genBasicTriplesMap; genTriplesMapWithJoin])
            | _ -> return! Gen.arrayOfLength arraySize genStringPoolStressTriplesMap
        }

    let genFunctionExecution =
        gen {
            let! functionIRI = gen {
                let! funcName = 
                    Gen.elements 
                        [
                            "concat"; 
                            "substring"; 
                            "uppercase"; 
                            "lowercase"; 
                            "trim"; 
                            "encode"; 
                            "decode"; 
                            "hash"
                        ]
                return sprintf "http://example.org/functions#%s" funcName
            }
            
            let! paramCount = Gen.choose(1, 3)
            let! inputs = Gen.listOfLength paramCount (gen {
                let! paramName = 
                    Gen.elements 
                        [
                            "input"; 
                            "value"; 
                            "source"; 
                            "target"; 
                            "format"; 
                            "encoding"
                        ]
                let! inputValue = genReference |> Gen.map Literal
                return {
                    Parameter = Some (sprintf "http://example.org/params#%s" paramName)
                    ParameterMap = None
                    InputValue = Some inputValue
                    InputValueMap = None
                    FunctionMap = None
                }
            })
            
            return {
                Function = Some functionIRI
                FunctionMap = None
                Input = inputs
            }
        }

    let genExpressionMapWithFunction =
        gen {
            let! functionExec = genFunctionExecution
            return {
                Template = None
                Reference = None
                Constant = None
                FunctionExecution = Some functionExec
            }
        }

    let genBuiltTriplesMap =
        gen {
            let! sourceData = gen {
                let! iterator = genComplexJSONPath
                let! refForm = Gen.elements [JSONPath; XPath; CSV]
                return (iterator, refForm)
            }
            
            let! subjectData = gen {
                let! template = genComplexTemplate
                let! classCount = Gen.choose(1, 3)
                let! classes = Gen.listOfLength classCount genClassIRI
                return (template, classes)
            }
            
            let! pomCount = Gen.choose(2, 5)
            let! pomData = Gen.listOfLength pomCount (gen {
                let! predicate = genPredicateIRI
                let! objRef = genReference
                let! hasDatatype = 
                    Gen.frequency [4, Gen.constant true; 6, Gen.constant false]
                let! datatype = if hasDatatype then genDatatype |> Gen.map Some else Gen.constant None
                return predicate, objRef, datatype
            })

            let iterator, refForm = sourceData
            let subjectTemplate, classes = subjectData

            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! RMLPipeline.DSL.iterator iterator
                    match refForm with
                    | JSONPath -> do! asJSONPath
                    | XPath -> do! asXPath
                    | CSV -> do! asCSV
                    | _ -> ()
                })
                
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI subjectTemplate)
                    for cls in classes do
                        do! addClass cls
                })
                
                for (predicate, objRef, datatype) in pomData do
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate predicate
                        do! addObjectMap (objectMap {
                            do! objectTermMap (refTermAsLiteral objRef)
                            match datatype with
                            | Some dt -> do! RMLPipeline.DSL.datatype dt
                            | None -> ()
                        })
                    })
            })
        }
    
    // Wrapper used to prevent FsCheck from using reflection on TriplesMap arrays
    type TriplesMapCollection = {
        Maps: TriplesMap[]
        Description: string
    }

    let genTriplesMapCollection =
        gen {
            let! collectionType = Gen.choose(1, 4)
            match collectionType with
            | 1 -> 
                // Simple collection
                let! count = Gen.choose(1, 3)
                let! maps = Gen.listOfLength count genBasicTriplesMap
                return {
                    Maps = maps |> List.toArray
                    Description = sprintf "Basic collection of %d maps" count
                }
            | 2 ->
                // Mixed complexity collection
                let! basicCount = Gen.choose(1, 2)
                let! joinCount = Gen.choose(1, 2)
                let! basicMaps = Gen.listOfLength basicCount genBasicTriplesMap
                let! joinMaps = Gen.listOfLength joinCount genTriplesMapWithJoin
                return {
                    Maps = (basicMaps @ joinMaps) |> List.toArray
                    Description = sprintf "Mixed collection: %d basic, %d with joins" basicCount joinCount
                }
            | 3 ->
                // Stress test collection
                let! count = Gen.choose(2, 4)
                let! maps = Gen.listOfLength count genStringPoolStressTriplesMap
                return {
                    Maps = maps |> List.toArray
                    Description = sprintf "Stress collection of %d complex maps" count
                }
            | _ ->
                // Built collection using DSL
                let! count = Gen.choose(2, 3)
                let! maps = Gen.listOfLength count genBuiltTriplesMap
                return {
                    Maps = maps |> List.toArray
                    Description = sprintf "DSL-built collection of %d maps" count
                }
        }

    type RMLCoreArbitraries =
        static member ExpressionMap() = Arb.fromGen genExpressionMap
        static member TermMap() = Arb.fromGen genTermMap
        static member LogicalSource() = Arb.fromGen genLogicalSource
        static member ObjectMap() = Arb.fromGen genObjectMap
        static member PredicateObjectMap() = Arb.fromGen genPredicateObjectMap
        static member SubjectMap() = Arb.fromGen genSubjectMap
        static member Join() = Arb.fromGen genJoin
        static member BasicTriplesMap() = Arb.fromGen genBasicTriplesMap
        static member TriplesMapWithJoin() = Arb.fromGen genTriplesMapWithJoin
        static member ComplexTriplesMap() = Arb.fromGen genComplexTriplesMap
        static member StringPoolStressTriplesMap() = Arb.fromGen genStringPoolStressTriplesMap
        static member TriplesMapArray() = Arb.fromGen genTriplesMapArray
        static member BuiltTriplesMap() = Arb.fromGen genBuiltTriplesMap
        static member FunctionExecution() = Arb.fromGen genFunctionExecution
        static member ExpressionMapWithFunction() = Arb.fromGen genExpressionMapWithFunction
        static member TriplesMapCollection() = Arb.fromGen genTriplesMapCollection  
