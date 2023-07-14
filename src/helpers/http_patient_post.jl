using Chain
using HTTP
using JSON
using Retry

function patient_post(url, headers, body; n_retries=3)
    @chain url begin
        try
            @repeat n_retries try
                HTTP.post(_, headers, body)
            catch e
                @delay_retry if (e isa HTTP.ConnectError) ||
                    (e isa HTTP.StatusError) &&
                                e.status < 200 &&
                                e.status >= 500
                end
            end
        catch
            error(HTTP.Exceptions.ConnectError("Failed to Connect to Server", 1))
        end
        # Parse response
        _.body
        String()
        JSON.parse()
    end
end
