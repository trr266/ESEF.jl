using Chain
using DataFrames
using DataFrameMacros
using HTTP

function query_twitter_user_profiles_api_call(twitter_handles)
    twitter_token = ENV["TWITTER_BEARER_TOKEN"]
    headers = ["Authorization" => "Bearer $(twitter_token)"]
    twitter_handles_csv_chunk = @chain twitter_handles join(",")
    r = HTTP.get(
        "https://api.twitter.com/2/users/by?usernames=$(twitter_handles_csv_chunk)&user.fields=public_metrics",
        headers,
    )

    d = JSON.parse(String(r.body))

    df = DataFrame()

    for r in d["data"]
        df_ = DataFrame(r)
        append!(df, df_; cols=:union)
    end

    return df
end

function query_twitter_user_profiles(twitter_handles_vector)
    df_twitter = DataFrame()

    for i in 1:ceil(length(twitter_handles_vector) / 100)
        base_index = 100 * (i - 1)
        twitter_handles_temp = @chain twitter_handles_vector _[Int(base_index + 1):min(
            length(twitter_handles_vector), Int(base_index + 100)
        )]
        df_twitter_temp = @chain twitter_handles_temp query_twitter_user_profiles_api_call()
        append!(df_twitter, df_twitter_temp)
    end

    df_twitter = @chain df_twitter @transform(
        :followers_count = :public_metrics["followers_count"]
    )

    return df_twitter
end
