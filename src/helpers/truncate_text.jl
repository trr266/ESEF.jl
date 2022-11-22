function truncate_text(string)
    if length(string) > 30
        return string[1:15] * "..." * string[(end - 14):end]
    else
        return string
    end
end
