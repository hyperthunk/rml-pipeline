namespace RMLPipeline

/// computation expressions for RML construction
module DSL =

    open RMLPipeline.Model

    // State monad ish thing
    type State<'S, 'T> = State of ('S -> 'T * 'S)
    
    // thanks for the insp, Haskell :)
    let runState (State f) state = f state
    let evalState (State f) state = fst (f state)
    let execState (State f) state = snd (f state)
    
    // State modification
    let get<'S> : State<'S, 'S> = State (fun s -> (s, s))
    let put<'S> (newState: 'S) : State<'S, unit> = State (fun _ -> ((), newState))
    let modify<'S> (f: 'S -> 'S) : State<'S, unit> = State (fun s -> ((), f s))
    
    // State computation expressions
    type StateBuilder() =
        member _.Return(x) = State (fun s -> (x, s))
        member _.ReturnFrom(m) = m
        member _.Bind(State f, k) = State (fun s -> 
            let (a, s') = f s
            let (State g) = k a
            g s')
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(State f, State g) = State (fun s ->
            let (_, s') = f s
            g s')
        member _.Delay(f) = State (fun s -> runState (f()) s)
        member _.For(seq, body) = 
            let folder state item = 
                let (_, newState) = runState (body item) state
                newState
            State (fun s -> ((), Seq.fold folder s seq))
        member _.While(guard, body) =
            let rec loop s =
                if guard() then
                    let (_, s') = runState body s
                    loop s'
                else ((), s)
            State loop

    let state = StateBuilder()

    // Lifting functions for convenience
    let lift f = State (fun s -> (f s, s))
    let lift2 f a b = state {
        let! x = a
        let! y = b
        return f x y
    }

    // Expression Map Builder
    type ExpressionMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.YieldFrom(m) = m

    let exprMap = ExpressionMapBuilder()

    // Expression Map operations
    let template (t: string) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Template = Some t })

    let reference (r: string) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Reference = Some r })

    let constant (c: RDFNode) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Constant = Some c })

    let constantURI (uri: string) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Constant = Some (URI uri) })

    let constantLiteral (lit: string) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Constant = Some (Literal lit) })

    let constantBlankNode (bn: string) : State<ExpressionMap, unit> = 
        modify (fun expr -> { expr with Constant = Some (BlankNode bn) })

    // Term Map Builder
    type TermMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.YieldFrom(m) = m

    let termMap = TermMapBuilder()

    // Term Map operations
    let expressionMap (exprMapComp: State<ExpressionMap, unit>) : State<TermMap, unit> = state {
        let initialExpr = Builders.emptyExpressionMap
        let finalExpr = execState exprMapComp initialExpr
        do! modify (fun term -> { term with ExpressionMap = finalExpr })
    }

    let termType (tt: TermType) : State<TermMap, unit> = 
        modify (fun term -> { term with TermType = Some tt })

    let asIRI : State<TermMap, unit> = termType IRITerm
    let asLiteral : State<TermMap, unit> = termType LiteralTerm
    let asBlankNode : State<TermMap, unit> = termType BlankNodeTerm
    let asURI : State<TermMap, unit> = termType URITerm

    // Subject Map Builder
    type SubjectMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.YieldFrom(m) = m

    let subjectMap = SubjectMapBuilder()

    // Subject Map operations
    let subjectTermMap (termMapComp: State<TermMap, unit>) : State<SubjectMap, unit> = state {
        let initialTerm = Builders.emptyTermMap
        let finalTerm = execState termMapComp initialTerm
        do! modify (fun subj -> { subj with SubjectTermMap = finalTerm })
    }

    let addClass (classIRI: IRI) : State<SubjectMap, unit> = 
        modify (fun subj -> { subj with Class = classIRI :: subj.Class })

    let addClasses (classes: IRI list) : State<SubjectMap, unit> = 
        modify (fun subj -> { subj with Class = classes @ subj.Class })

    // Object Map Builder
    type ObjectMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.YieldFrom(m) = m

    let objectMap = ObjectMapBuilder()

    // Object Map operations
    let objectTermMap (termMapComp: State<TermMap, unit>) : State<ObjectMap, unit> = state {
        let initialTerm = Builders.emptyTermMap
        let finalTerm = execState termMapComp initialTerm
        do! modify (fun obj -> { obj with ObjectTermMap = finalTerm })
    }

    let datatype (dt: IRI) : State<ObjectMap, unit> = 
        modify (fun obj -> { obj with Datatype = Some dt })

    let language (lang: LanguageTag) : State<ObjectMap, unit> = 
        modify (fun obj -> { obj with Language = Some lang })

    // Predicate Map Builder
    type PredicateMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.YieldFrom(m) = m

    let predicateMap = PredicateMapBuilder()

    // Predicate Map operations
    let predicateTermMap (termMapComp: State<TermMap, unit>) : State<PredicateMap, unit> = state {
        let initialTerm = Builders.emptyTermMap
        let finalTerm = execState termMapComp initialTerm
        do! modify (fun pred -> { pred with PredicateTermMap = finalTerm })
    }

    // Join Builder
    type JoinBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))

    let join = JoinBuilder()

    // Join operations
    let child (c: string) : State<Join, unit> = 
        modify (fun j -> { j with Child = Some c })

    let parent (p: string) : State<Join, unit> = 
        modify (fun j -> { j with Parent = Some p })

    // Reference Object Map Builder
    type RefObjectMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))

    let refObjectMap = RefObjectMapBuilder()

    // Reference Object Map operations
    let parentTriplesMap (ptm: TriplesMap) : State<RefObjectMap, unit> = 
        modify (fun rom -> { rom with ParentTriplesMap = ptm })

    let addJoinCondition (joinComp: State<Join, unit>) : State<RefObjectMap, unit> = state {
        let initialJoin = { Child = None; Parent = None; ChildMap = None; ParentMap = None }
        let finalJoin = execState joinComp initialJoin
        do! modify (fun rom -> { rom with JoinCondition = finalJoin :: rom.JoinCondition })
    }

    // Predicate Object Map Builder
    type PredicateObjectMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.For(seq, body) = state {
            for item in seq do
                do! body item
        }

    let predicateObjectMap = PredicateObjectMapBuilder()

    // Predicate Object Map operations
    let addPredicate (pred: IRI) : State<PredicateObjectMap, unit> = 
        modify (fun pom -> { pom with Predicate = pred :: pom.Predicate })

    let addPredicates (preds: IRI list) : State<PredicateObjectMap, unit> = 
        modify (fun pom -> { pom with Predicate = preds @ pom.Predicate })

    let addPredicateMap (predMapComp: State<PredicateMap, unit>) : State<PredicateObjectMap, unit> = state {
        let initialPred = { PredicateTermMap = Builders.emptyTermMap }
        let finalPred = execState predMapComp initialPred
        do! modify (fun pom -> { pom with PredicateMap = finalPred :: pom.PredicateMap })
    }

    let addObject (obj: RDFNode) : State<PredicateObjectMap, unit> = 
        modify (fun pom -> { pom with Object = obj :: pom.Object })

    let addObjectURI (uri: string) : State<PredicateObjectMap, unit> = 
        modify (fun pom -> { pom with Object = (URI uri) :: pom.Object })

    let addObjectLiteral (lit: string) : State<PredicateObjectMap, unit> = 
        modify (fun pom -> { pom with Object = (Literal lit) :: pom.Object })

    let addObjectMap (objMapComp: State<ObjectMap, unit>) : State<PredicateObjectMap, unit> = state {
        let initialObj = { 
            ObjectTermMap = Builders.emptyTermMap
            Datatype = None
            DatatypeMap = None
            Language = None
            LanguageMap = None 
        }
        let finalObj = execState objMapComp initialObj
        do! modify (fun pom -> { pom with ObjectMap = finalObj :: pom.ObjectMap })
    }

    let addRefObjectMap (ptm: TriplesMap) (refObjMapComp: State<RefObjectMap, unit>) : State<PredicateObjectMap, unit> = state {
        let initialRef = { ParentTriplesMap = ptm; JoinCondition = [] }
        let finalRef = execState refObjMapComp initialRef
        do! modify (fun pom -> { pom with RefObjectMap = finalRef :: pom.RefObjectMap })
    }

    // Logical Source Builder
    type LogicalSourceBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))

    let logicalSource = LogicalSourceBuilder()

    // Logical Source operations
    let iterator (iter: string) : State<AbstractLogicalSource, unit> = 
        modify (fun ls -> { ls with SourceIterator = Some iter })

    let referenceFormulation (rf: ReferenceFormulation) : State<AbstractLogicalSource, unit> = 
        modify (fun ls -> { ls with SourceReferenceFormulation = Some rf })

    let asJSONPath : State<AbstractLogicalSource, unit> = referenceFormulation JSONPath
    let asXPath : State<AbstractLogicalSource, unit> = referenceFormulation XPath
    let asCSV : State<AbstractLogicalSource, unit> = referenceFormulation CSV

    // Triples Map Builder
    type TriplesMapBuilder() =
        member _.Return(_) = State (fun s -> ((), s))
        member _.ReturnFrom(m) = m
        member _.Bind(m, f) = state {
            do! m
            return! f ()
        }
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(m1, m2) = state {
            do! m1
            do! m2
        }
        member _.Delay(f) = state { return! f() }
        member _.Yield(_) = State (fun s -> ((), s))
        member _.For(seq, body) = state {
            for item in seq do
                do! body item
        }

    let triplesMap = TriplesMapBuilder()

    // Triples Map operations
    let setLogicalSource (logicalSourceComp: State<AbstractLogicalSource, unit>) : State<TriplesMap, unit> = state {
        let initialLS = { SourceIterator = None; SourceReferenceFormulation = None }
        let finalLS = execState logicalSourceComp initialLS
        do! modify (fun tm -> { tm with LogicalSource = finalLS })
    }

    let setSubjectMap (subjectMapComp: State<SubjectMap, unit>) : State<TriplesMap, unit> = state {
        let initialSubj = { SubjectTermMap = Builders.emptyTermMap; Class = []; GraphMap = [] }
        let finalSubj = execState subjectMapComp initialSubj
        do! modify (fun tm -> { tm with SubjectMap = Some finalSubj })
    }

    let setSubject (subj: IRI) : State<TriplesMap, unit> = 
        modify (fun tm -> { tm with Subject = Some subj })

    let addPredicateObjectMap (pomComp: State<PredicateObjectMap, unit>) : State<TriplesMap, unit> = state {
        let initialPOM = { 
            Predicate = []
            PredicateMap = []
            Object = []
            ObjectMap = []
            RefObjectMap = []
            GraphMap = []
        }
        let finalPOM = execState pomComp initialPOM
        do! modify (fun tm -> { tm with PredicateObjectMap = finalPOM :: tm.PredicateObjectMap })
    }

    let baseIRI (iri: IRI) : State<TriplesMap, unit> = 
        modify (fun tm -> { tm with BaseIRI = Some iri })

    // Vocabulary shortcuts
    [<AutoOpen>]
    module Vocab =
        let rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        let foafPerson = "http://xmlns.com/foaf/0.1/Person"
        let foafName = "http://xmlns.com/foaf/0.1/name"
        let foafAge = "http://xmlns.com/foaf/0.1/age"
        let foafEmail = "http://xmlns.com/foaf/0.1/email"
        let schemaPerson = "http://schema.org/Person"
        let schemaName = "http://schema.org/name"
        let xsdString = "http://www.w3.org/2001/XMLSchema#string"
        let xsdInteger = "http://www.w3.org/2001/XMLSchema#integer"
        let xsdDateTime = "http://www.w3.org/2001/XMLSchema#dateTime"

    // Helper functions for common patterns
    [<AutoOpen>]
    module Expressions =
        
        /// Create a reference-based expression map
        let refExpr (ref: string) = exprMap {
            do! reference ref
        }
        
        /// Create a template-based expression map
        let templateExpr (tmpl: string) = exprMap {
            do! template tmpl
        }
        
        /// Create a constant URI expression map
        let uriExpr (uri: string) = exprMap {
            do! constantURI uri
        }
        
        /// Create a simple reference term map as IRI
        let refTermAsIRI (ref: string) = termMap {
            do! expressionMap (exprMap {
                do! reference ref
            })
            do! asIRI
        }
        
        /// Create a simple template term map as IRI
        let templateTermAsIRI (tmpl: string) = termMap {
            do! expressionMap (exprMap {
                do! template tmpl
            })
            do! asIRI
        }
        
        /// Create a simple reference term map as literal
        let refTermAsLiteral (ref: string) = termMap {
            do! expressionMap (exprMap {
                do! reference ref
            })
            do! asLiteral
        }
        
        /// Create a simple predicate-object mapping
        let simplePredObj (predIRI: IRI) (objRef: string) = predicateObjectMap {
            do! addPredicate predIRI
            do! addObjectMap (objectMap {
                do! objectTermMap (refTermAsLiteral objRef)
            })
        }
        
        /// Create a typed literal object mapping
        let typedPredObj (predIRI: IRI) (objRef: string) (datatypeIRI: IRI) = predicateObjectMap {
            do! addPredicate predIRI
            do! addObjectMap (objectMap {
                do! objectTermMap (refTermAsLiteral objRef)
                do! datatype datatypeIRI
            })
        }
        
        /// Create a reference object mapping with join condition    
        let buildExpressionMap (comp: State<ExpressionMap, unit>) : ExpressionMap =
            execState comp Builders.emptyExpressionMap
        
        /// Create a term map from a computation expression
        let buildTermMap (comp: State<TermMap, unit>) : TermMap =
            execState comp Builders.emptyTermMap
        
        /// Create a subject map from a computation expression
        let buildSubjectMap (comp: State<SubjectMap, unit>) : SubjectMap =
            let initial = { SubjectTermMap = Builders.emptyTermMap; Class = []; GraphMap = [] }
            execState comp initial
        
        /// Create an object map from a computation expression
        let buildObjectMap (comp: State<ObjectMap, unit>) : ObjectMap =
            let initial = { 
                ObjectTermMap = Builders.emptyTermMap
                Datatype = None
                DatatypeMap = None
                Language = None
                LanguageMap = None 
            }
            execState comp initial
        
        /// Create a predicate map from a computation expression
        let buildPredicateMap (comp: State<PredicateMap, unit>) : PredicateMap =
            let initial = { PredicateTermMap = Builders.emptyTermMap }
            execState comp initial
        
        /// Create a join from a computation expression
        let buildPredicateObjectMap (comp: State<PredicateObjectMap, unit>) : PredicateObjectMap =
            let initial = { 
                Predicate = []
                PredicateMap = []
                Object = []
                ObjectMap = []
                RefObjectMap = []
                GraphMap = []
            }
            execState comp initial
        
        /// Create a reference object map from a computation expression
        let buildLogicalSource (comp: State<AbstractLogicalSource, unit>) : AbstractLogicalSource =
            let initial = { SourceIterator = None; SourceReferenceFormulation = None }
            execState comp initial
        
        /// Create a reference object map from a computation expression
        let buildTriplesMap (comp: State<TriplesMap, unit>) : TriplesMap =
            let initial = { 
                LogicalSource = { SourceIterator = None; SourceReferenceFormulation = None }
                SubjectMap = None
                Subject = None
                PredicateObjectMap = []
                BaseIRI = None
                LogicalTarget = None
            }
            execState comp initial

    (* 
    /// Example 1: Simple Person mapping using computation expressions
    let personTriplesMapComp = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.people[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
            do! addClass foafPerson
        })
        
        do! addPredicateObjectMap (simplePredObj foafName "name")
        do! addPredicateObjectMap (typedPredObj foafAge "age" xsdInteger)
        do! addPredicateObjectMap (simplePredObj foafEmail "email")
    }

    let personTriplesMap = buildTriplesMap personTriplesMapComp

    /// Example 2: More complex mapping with multiple classes and custom logic
    let complexPersonMapping = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.employees[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (termMap {
                do! expressionMap (exprMap {
                    do! template "http://company.org/employee/{employeeId}"
                })
                do! asIRI
            })
            do! addClass "http://company.org/Employee"
            do! addClass foafPerson
            do! addClass schemaPerson
        })
        
        // Multiple predicate-object mappings
        for (predicate, reference, datatypeOpt) in [
            (foafName, "fullName", None)
            ("http://company.org/employeeId", "employeeId", Some xsdString)
            ("http://company.org/startDate", "startDate", Some xsdDateTime)
            (foafAge, "age", Some xsdInteger)
        ] do
            match datatypeOpt with
            | Some dt -> do! addPredicateObjectMap (typedPredObj predicate reference dt)
            | None -> do! addPredicateObjectMap (simplePredObj predicate reference)
        
        do! baseIRI "http://company.org/"
    }

    let complexPersonTriplesMap = buildTriplesMap complexPersonMapping

    /// Example 3: Join condition example
    let departmentMapping = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.departments[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (templateTermAsIRI "http://company.org/department/{id}")
        })
        
        do! addPredicateObjectMap (simplePredObj "http://company.org/departmentName" "name")
    }

    let departmentTriplesMap = buildTriplesMap departmentMapping

    let employeeWithDepartmentMapping = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.employees[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (templateTermAsIRI "http://company.org/employee/{id}")
        })
        
        do! addPredicateObjectMap (simplePredObj foafName "name")
        
        // Join to department
        do! addPredicateObjectMap (predicateObjectMap {
            do! addPredicate "http://company.org/worksInDepartment"
            do! addRefObjectMap (refObjectMap {
                do! addJoinCondition (join {
                    do! child "departmentId"
                    do! parent "id"
                })
            }) departmentTriplesMap
        })
    }

    let employeeWithDepartmentTriplesMap = buildTriplesMap employeeWithDepartmentMapping

    /// Example 4: Conditional mappings
    let conditionalMapping = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.people[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
            do! addClass foafPerson
        })
        
        // Basic mappings
        do! addPredicateObjectMap (simplePredObj foafName "name")
        
        // Conditional email mapping
        do! addPredicateObjectMap (predicateObjectMap {
            do! addPredicate foafEmail
            do! addObjectMap (objectMap {
                do! objectTermMap (termMap {
                    do! expressionMap (exprMap {
                        do! reference "email"
                    })
                    do! asLiteral
                })
            })
        })
    }

    let conditionalTriplesMap = buildTriplesMap conditionalMapping

    /// Example 5: Demonstration of composability
    let createPersonWithContactInfo name email age = triplesMap {
        do! setLogicalSource (logicalSource {
            do! iterator "$.contacts[*]"
            do! asJSONPath
        })
        
        do! setSubjectMap (subjectMap {
            do! subjectTermMap (templateTermAsIRI "http://contacts.org/person/{id}")
            do! addClass foafPerson
        })
        
        do! addPredicateObjectMap (simplePredObj foafName name)
        do! addPredicateObjectMap (simplePredObj foafEmail email)
        do! addPredicateObjectMap (typedPredObj foafAge age xsdInteger)
    }

    let contactTriplesMap = buildTriplesMap (createPersonWithContactInfo "fullName" "emailAddress" "years") 
    *)