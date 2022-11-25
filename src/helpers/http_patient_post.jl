using Chain
using HTTP
using JSON
using Retry

function patient_post(url, headers, body; n_retries=3)
    @chain url begin
        # Query wikidata sparql url
        @repeat n_retries try
            HTTP.post(_, headers, body)
        catch e
            @delay_retry if http_status(e) < 200 &&
                            http_status(e) >= 500 end
        end

        # Parse response
        _.body
        String()
        JSON.parse()
    end
end

