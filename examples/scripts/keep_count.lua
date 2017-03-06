count_per_tick = 0

function process_metric(pyld)
   count_per_tick = count_per_tick + 1
end

function process_log(pyld)
   count_per_tick = count_per_tick + 1
end

function tick(pyld)
   payload.push_metric(pyld, "count_per_tick", count_per_tick)
   payload.push_log(pyld, string.format("count_per_tick: %s", count_per_tick))
   count_per_tick = 0
end 
