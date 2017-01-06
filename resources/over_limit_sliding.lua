local slice = {60, 3600, 86400}
local precision = {1, 60, 1800}
local dkeys = {'m', 'h', 'd'}
local ts = tonumber(table.remove(ARGV))
local weight = tonumber(table.remove(ARGV))
local fail = false

-- Make two passes, the first to clean out old data and make sure there is
-- enough available resources, the second to update the counts.
for _, ready in ipairs({false, true}) do
    -- iterate over all of the limits provided
    for i = 1, math.min(#ARGV, #slice) do
        local limit = tonumber(ARGV[i])

        -- make sure that it is a limit we should check
        if limit > 0 then
            -- calculate the cutoff times and suffixes for the keys
            local cutoff = ts - slice[i]
            local curr = '' .. (precision[i] * math.floor(ts / precision[i]))
            local suff = ':' .. dkeys[i]
            local suff2 = suff .. ':l'

            -- check each key to verify it is not above the limit
            for j, k in ipairs(KEYS) do
                local key = k .. suff
                local key2 = k .. suff2

                if ready then
                    -- if we get here, our limits are fine
                    redis.call('incrby', key, weight)
                    local oldest = redis.call('lrange', key2, '0', '1')
                    if oldest[2] == curr then
                        redis.call('ltrim', key2, 0, -3)
                        redis.call('rpush', key2, weight + tonumber(oldest[1]), oldest[2])
                    else
                        redis.call('rpush', key2, weight, curr)
                    end
                    redis.call('expire', key, slice[i])
                    redis.call('expire', key2, slice[i])

                else
                    -- get the current counted total
                    local total = tonumber(redis.call('get', key) or '0')

                    -- only bother to clean out old data on our first pass through,
                    -- we know the second pass won't do anything
                    while total + weight > limit do
                        local oldest = redis.call('lrange', key2, '0', '1')
                        if #oldest == 0 then
                            break
                        end
                        if tonumber(oldest[2]) <= cutoff then
                            total = tonumber(redis.call('incrby', key, -tonumber(oldest[1])))
                            redis.call('ltrim', key2, '2', '-1')
                        else
                            break
                        end
                    end

                    fail = fail or total + weight > limit
                end
            end
        end
    end
    if fail then
        break
    end
end

return fail