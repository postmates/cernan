function process_metric(pyld)
   local old_name = payload.metric_name(pyld, 1)
   local collectd, rest = string.match(old_name, "^(collectd)[%.@][%w_]+(.*)")
   if collectd ~= nil then
      local new_name = string.format("%s%s", collectd, rest)
      payload.set_metric_name(pyld, 1, new_name)
   end
end

function process_log(pyld)
end

function tick(pyld)
end 
