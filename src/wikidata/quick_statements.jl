using Chain

function build_quick_statement(subject::String, predicate::String, object::String)
    join([subject, predicate, object], "\t")
end

function build_quick_statement(subject::String, predicate::Vector, object::Vector)
    length(predicate) == length(object) || throw(ArgumentError("Predicate and object vectors must be of equal length"))

    build_quick_statement.((subject,), predicate, object)
end

function build_quick_statement(predicate::Vector, object::Vector)
    join(["CREATE", build_quick_statement("LAST", predicate, object)...], "\n")
end

function compose_merge_statement(wd_entity_vector)
    qs_statements = []
    for merge_ in wd_entity_vector[2:end]
        push!(qs_statements, "MERGE\t$(wd_entity_vector[1])\t$(merge_)")
    end

    return qs_statements
end

