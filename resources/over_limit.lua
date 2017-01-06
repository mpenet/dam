local slice = {1, 60, 3600, 86400}
local dkeys = {'s', 'm', 'h', 'd'}
local ts = tonumber(table.remove(ARGV))
local weight = tonumber(table.remove(ARGV))
local fail = false

-- only update the counts if all of the limits are okay
for _, ready in ipairs({false, true}) do
    for i = 1, math.min(#ARGV, #slice) do
        local limit = tonumber(ARGV[i])

        -- only check limits that are worthwhile
        if limit > 0 then
            local suff = ':' .. dkeys[i] .. ':' .. math.floor(ts / slice[i])
            local remain = 1 + slice[i] - math.fmod(ts, slice[i])
            for j, k in ipairs(KEYS) do
                local key = k .. suff
                if ready then
                    redis.call('incrby', key, weight)
                    redis.call('expire', key, remain)
                else
                    local total = tonumber(redis.call('get', key) or '0')
                    if total + weight > limit then
                        fail = true
                        break
                    end
                end
            end
        end
    end
    if fail then
        break
    end
end

return fail