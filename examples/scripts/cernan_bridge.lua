count_per_tick = 0

function process_metric(pyld)
   count_per_tick = count_per_tick + 1

   local old_name = payload.metric_name(pyld, 1)
   local collectd, rest = string.match(old_name, "^(collectd)[%.@][%w_]+(.*)")
   if collectd ~= nil then
      local new_name = string.format("%s%s", collectd, rest)
      payload.set_metric_name(pyld, 1, new_name)
   end
end

function process_log(pyld)
   count_per_tick = count_per_tick + 1
end

function tick(pyld)
   payload.push_metric(pyld, "cernan_bridge.count_per_tick", count_per_tick)
   payload.push_log(pyld, string.format("count_per_tick: %s", count_per_tick))
   count_per_tick = 0
end 
