function process(n)
   local old_name = payload.metric_name(n)
   local collectd, rest = string.match(old_name, "^(collectd)[%.@][%w_]+(.*)")
   if collectd ~= nil then
      local new_name = string.format("%s%s", collectd, rest)
      payload.set_metric_name(n, new_name)
   end
end
