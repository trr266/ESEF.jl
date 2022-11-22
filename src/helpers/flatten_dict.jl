function rec_flatten_dict(d, prefix_delim=".")
    # Source: https://discourse.julialang.org/t/question-is-there-a-function-to-flatten-a-nested-dictionary/54462/2
    new_d = empty(d)
    for (key, value) in pairs(d)
        if isa(value, Dict)
            flattened_value = rec_flatten_dict(value, prefix_delim)
            for (ikey, ivalue) in pairs(flattened_value)
                new_d["$key.$ikey"] = ivalue
            end
        else
            new_d[key] = value
        end
    end
    return new_d
end
